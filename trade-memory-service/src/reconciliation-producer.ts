import { Kafka, Producer } from 'kafkajs';
import { ReconciliationMessage } from './types.js';

const kafka = new Kafka({
  clientId: 'trade-memory-service-reconciliation-producer',
  brokers: ['kafka:9092'],
  retry: {
    initialRetryTime: 100,
    retries: 5,
  },
  connectionTimeout: 10000,
  requestTimeout: 30000,
});

let producer: Producer | null = null;

export async function getReconciliationProducer(): Promise<Producer> {
  if (!producer) {
    producer = kafka.producer();
    await producer.connect();
    console.log('Reconciliation producer connected');
  }
  return producer;
}

export async function publishReconciliationMessage(tradeTime: Date): Promise<void> {
  try {
    const prod = await getReconciliationProducer();
    const message: ReconciliationMessage = {
      tradeTime: tradeTime.toISOString(),
      detectedAt: new Date().toISOString(),
    };

    await prod.send({
      topic: 'reconciliation',
      messages: [
        {
          value: JSON.stringify(message),
          timestamp: Date.now().toString(),
        },
      ],
    });

    console.log(`[RECONCILIATION] Published reconciliation message for trade time: ${tradeTime.toISOString()}`);
  } catch (error) {
    console.error('[RECONCILIATION] Error publishing reconciliation message:', error);
  }
}

export async function closeReconciliationProducer(): Promise<void> {
  if (producer) {
    await producer.disconnect();
    producer = null;
    console.log('Reconciliation producer disconnected');
  }
}
