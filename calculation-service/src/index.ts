import { Kafka, Consumer } from 'kafkajs';
import { connectToDatabase, closeDatabase } from './db.js';
import { RawMarketMessage, isParsedRawMarketMessage, ReconciliationMessage, isParsedReconciliationMessage } from './types.js';
import { createGrpcClient, closeGrpcClient } from './grpc-client.js';
import { processMarketMessage } from './market-buffer.js';
import { setKafkaConsumer, initPartitionTracking, commitOffsetsInOrder, 
  checkOffsetIsCompleted, checkOffsetIsInFlight, addOffsetToInFlight,
  removeOffsetFromInFlight, 
  addOffsetToCompleted} from './offset-handler.js';
import { addReconciliationTimestamp, processPendingReconciliations, getReconciliationTimestamps } from './reconciliation-handler.js';
import { setReconciliationConsumer, initReconciliationPartitionTracking } from './reconciliation-offset-handler.js';

const kafka = new Kafka({
  clientId: 'calculation-service',
  brokers: ['kafka:9092'],
  retry: {
    initialRetryTime: 100,
    retries: 5,
  },
  connectionTimeout: 10000,
  requestTimeout: 30000,
});

const consumer: Consumer = kafka.consumer({
  groupId: 'calculation-service-group', 
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
  maxWaitTimeInMs: 5000,
  retry: {
    initialRetryTime: 100,
    retries: 5,
  },
});

const reconciliationConsumer: Consumer = kafka.consumer({
  groupId: 'calculation-service-reconciliation-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
  maxWaitTimeInMs: 5000,
  retry: {
    initialRetryTime: 100,
    retries: 5,
  },
});

let reconciliationInterval: NodeJS.Timeout | null = null;

function startReconciliationInterval(): void {
  if (reconciliationInterval) {
    clearInterval(reconciliationInterval);
    reconciliationInterval = null;
  }

  reconciliationInterval = setInterval(() => {
    processPendingReconciliations().catch((error) => {
      console.error('[RECONCILIATION] Error in periodic reconciliation processing:', error);
    });
  }, 10000);
}

function stopReconciliationInterval(): void {
  if (reconciliationInterval) {
    clearInterval(reconciliationInterval);
    reconciliationInterval = null;
  }
}

process.on('SIGTERM', async () => {
  console.log('Received SIGTERM, shutting down gracefully...');
  await shutdown();
  stopReconciliationInterval();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('Received SIGINT, shutting down gracefully...');
  await shutdown();
  stopReconciliationInterval();
  process.exit(0);
});

async function shutdown(): Promise<void> {
  try {
    await Promise.all([consumer.disconnect(), reconciliationConsumer.disconnect()]);
    closeGrpcClient();
    await closeDatabase();
    console.log('Shutdown complete');
  } catch (error) {
    console.error('Error during shutdown:', error);
  }
}

async function runConsumer(): Promise<void> {
  const maxRetries = 5;
  let attempt = 0;

  while (attempt < maxRetries) {
    try {
      console.log('Waiting for Kafka to be ready...');
      await new Promise(resolve => setTimeout(resolve, 10000));

      // Initialize gRPC client
      createGrpcClient();
      
      await Promise.all([connectToDatabase(), consumer.connect(), reconciliationConsumer.connect()]);
      setKafkaConsumer(consumer);
      setReconciliationConsumer(reconciliationConsumer);

      console.log('Connected to Kafka');
      
      await consumer.subscribe({ topics: ['market'], fromBeginning: false });
      await reconciliationConsumer.subscribe({ topics: ['reconciliation'], fromBeginning: false });

      startReconciliationInterval();
      console.log('Reconciliation interval started');

      // Start consumer - it should run indefinitely
      await consumer.run({
        autoCommit: false, // Manual commit after successful processing
        eachMessage: async ({ topic, partition, message }) => {
          if (!message.value) {
            console.warn(`[MARKET] [DLQ] Received message with no value at partition ${partition}, offset ${message.offset}`);
            return;
          }

          try {
            const parsedMessage: RawMarketMessage = JSON.parse(
              message.value.toString()
            );
            
            if (!isParsedRawMarketMessage(parsedMessage)) {
              console.warn(`[MARKET] [DLQ] Received invalid market message at partition ${partition}, offset ${message.offset}`);
              return;
            }

            initPartitionTracking(partition);
              
            const currentOffset = message.offset;
              
            // Check if this offset is already being processed
            if (checkOffsetIsInFlight(partition, currentOffset) || checkOffsetIsCompleted(partition, currentOffset)) {
              return;
            }

            // Add to in-flight
            addOffsetToInFlight(partition, currentOffset);

            processMarketMessage(
              parsedMessage,
              partition,
              currentOffset
            ).then((skipped) => {
              removeOffsetFromInFlight(partition, currentOffset);
              addOffsetToCompleted(partition, currentOffset);

              // Try to commit offsets in order
              commitOffsetsInOrder(partition).catch((error) => {
                console.error(`[MARKET] Error committing offsets for partition ${partition}:`, error);
              });
            }).catch((error) => {
              removeOffsetFromInFlight(partition, currentOffset);
              console.error(
                `[MARKET] Error processing market message at partition ${partition}, offset ${currentOffset}:`,
                error
              );
              throw error;
            });
          } catch (error) {
            console.error(
              `[MARKET] [DLQ] Error processing market message at partition ${partition}, offset ${message.offset}:`,
              error
            );
          }
        },
      });

      // Start reconciliation consumer
      await reconciliationConsumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
          if (!message.value) {
            console.warn(`[RECONCILIATION] Received message with no value at partition ${partition}, offset ${message.offset}`);
            return;
          }

          try {
            const parsedMessage: ReconciliationMessage = JSON.parse(
              message.value.toString()
            );

            if (!isParsedReconciliationMessage(parsedMessage)) {
              console.warn(`[RECONCILIATION] Received invalid reconciliation message at partition ${partition}, offset ${message.offset} - would send to DLQ`);
              return;
            }

            initReconciliationPartitionTracking(partition);
            addReconciliationTimestamp(parsedMessage.tradeTime, partition, message.offset);
          } catch (error) {
            console.error(
              `[RECONCILIATION] Error processing reconciliation message at partition ${partition}, offset ${message.offset}:`,
              error
            );
          }
        },
      });

      console.log('Market consumer started successfully');
      console.log('Reconciliation consumer started successfully');
      
      break;
    } catch (error) {
      attempt++;
      console.error(
        `Error in Kafka consumer (attempt ${attempt}/${maxRetries}):`,
        error
      );

      try {
        await Promise.allSettled([consumer.disconnect(), reconciliationConsumer.disconnect(), closeDatabase()]);
      } catch (e) {
        // Ignore disconnect errors
      }

      if (attempt < maxRetries) {
        const waitTime = Math.min(1000 * Math.pow(2, attempt), 30000);
        console.log(`Retrying in ${waitTime}ms...`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
      } else {
        console.error('Max retries reached, giving up');
        process.exit(1);
      }
    }
  }
}

// Start service
async function main(): Promise<void> {
  try {
    console.log('Calculation service starting...');
    
    await runConsumer();
  } catch (error) {
    console.error('Fatal error in main:', error);
    if (error instanceof Error) {
      console.error('Error stack:', error.stack);
    }
    process.exit(1);
  }
}

main();
