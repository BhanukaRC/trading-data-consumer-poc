import { Kafka } from 'kafkajs';
import { StreamProcessor } from './StreamProcessor';
import { RawMarketMessage, RawTradeMessage } from './types';
import { createStreamGenerator, StreamConfig } from './stream-generator';
import { Readable } from 'stream';
import { ReadableStream } from 'stream/web';

type RawMessage = RawMarketMessage | RawTradeMessage;

// Configuration from environment variables with defaults
const streamConfig: StreamConfig = {
    tradesPerSecond: parseInt(process.env.TRADES_PER_SECOND || '100'),
    tradesPerMarketMessage: parseInt(process.env.TRADES_PER_MARKET_MESSAGE || '300'),
    outOfOrderProbability: parseFloat(process.env.OUT_OF_ORDER_PROBABILITY || '0.05')
};

console.log('Stream Configuration:', {
    tradesPerSecond: streamConfig.tradesPerSecond,
    tradesPerMarketMessage: streamConfig.tradesPerMarketMessage,
    outOfOrderProbability: streamConfig.outOfOrderProbability,
});

const kafka = new Kafka({
    clientId: 'kafka-producer',
    brokers: ['kafka:9092'],
    retry: {
        initialRetryTime: 100,
        retries: 5,
    },
    connectionTimeout: 10000,
    requestTimeout: 30000,
});

const producer = kafka.producer();

process.on('SIGTERM', async () => {
    console.log('Received SIGTERM, shutting down gracefully...');
    try {
        await producer.disconnect();
        console.log('Producer disconnected');
        process.exit(0);
    } catch (error) {
        console.error('Error during shutdown:', error);
        process.exit(1);
    }
});

process.on('SIGINT', async () => {
    console.log('Received SIGINT, shutting down gracefully...');
    try {
        await producer.disconnect();
        console.log('Producer disconnected');
        process.exit(0);
    } catch (error) {
        console.error('Error during shutdown:', error);
        process.exit(1);
    }
});

async function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function onMessage(message: RawMessage) {
    await producer.send({
        topic: message.messageType,
        messages: [
            {
                value: JSON.stringify(message),
                timestamp: Date.now().toString(),
            }
        ],
    });
}

function createReadableStreamFromGenerator(generator: Readable): ReadableStream<Uint8Array> {
    return new ReadableStream({
        start(controller) {
            generator.on('data', (chunk: Buffer) => {
                controller.enqueue(new Uint8Array(chunk));
            });

            generator.on('end', () => {
                controller.close();
            });

            generator.on('error', (error) => {
                controller.error(error);
            });
        },
        cancel() {
            generator.destroy();
        },
    });
}

async function generateStreamAndProduce() {
    const streamGenerator = createStreamGenerator(streamConfig);
    const readableStream = createReadableStreamFromGenerator(streamGenerator);

    const streamProcessor = new StreamProcessor(onMessage);

    await streamProcessor.processStream(readableStream);

    console.log('Streaming ended');
    await producer.disconnect();
}

async function main() {
    const maxRetries = 5;
    let attempt = 0;

    while (attempt < maxRetries) {
        try {
            // Wait for Kafka to be ready
            console.log('Waiting for Kafka to be ready...');
            await sleep(10000);

            await producer.connect();
            console.log('Kafka Producer is ready');

            await generateStreamAndProduce();
            console.log('Stream processing completed');
            break;
        } catch (error) {
            attempt++;
            console.error(`Error connecting to Kafka (attempt ${attempt}/${maxRetries}):`, error);

            // Disconnect producer before retrying
            try {
                await producer.disconnect();
            } catch (e) {
                // Ignore disconnect errors
            }

            if (attempt < maxRetries) {
                const waitTime = Math.min(2000 * Math.pow(2, attempt), 30000);
                console.log(`Retrying in ${waitTime}ms...`);
                await sleep(waitTime);
            } else {
                console.error('Max retries reached, giving up');
                process.exit(1);
            }
        }
    }
}

main();