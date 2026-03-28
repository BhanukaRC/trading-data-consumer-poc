import { Kafka, Producer } from 'kafkajs';

const MARKET_DLQ_TOPIC = process.env.MARKET_DLQ_TOPIC || 'market-dlq';
const RECONCILIATION_DLQ_TOPIC = process.env.RECONCILIATION_DLQ_TOPIC || 'reconciliation-dlq';

const kafka = new Kafka({
  clientId: 'calculation-service-dlq',
  brokers: ['kafka:9092'],
  retry: {
    initialRetryTime: 100,
    retries: 5,
  },
  connectionTimeout: 10000,
  requestTimeout: 30000,
});

let producer: Producer | null = null;

export type DlqEnvelope = {
  sourceTopic: string;
  partition: number;
  offset: string;
  reason: string;
  errorMessage?: string;
  /** Original payload as UTF-8 string when available (may be truncated by caller). */
  valuePreview?: string;
  detectedAt: string;
};

async function getProducer(): Promise<Producer> {
  if (!producer) {
    producer = kafka.producer();
    await producer.connect();
    console.log('[DLQ] Producer connected');
  }
  return producer;
}

export async function publishMarketDlq(envelope: DlqEnvelope): Promise<void> {
  const prod = await getProducer();
  await prod.send({
    topic: MARKET_DLQ_TOPIC,
    messages: [
      {
        value: JSON.stringify(envelope),
        timestamp: Date.now().toString(),
      },
    ],
  });
  console.warn(
    `[DLQ] Sent market dead-letter: partition=${envelope.partition} offset=${envelope.offset} reason=${envelope.reason}`
  );
}

export async function publishReconciliationDlq(envelope: DlqEnvelope): Promise<void> {
  const prod = await getProducer();
  await prod.send({
    topic: RECONCILIATION_DLQ_TOPIC,
    messages: [
      {
        value: JSON.stringify(envelope),
        timestamp: Date.now().toString(),
      },
    ],
  });
  console.warn(
    `[DLQ] Sent reconciliation dead-letter: partition=${envelope.partition} offset=${envelope.offset} reason=${envelope.reason}`
  );
}

export async function closeDlqProducer(): Promise<void> {
  if (producer) {
    await producer.disconnect();
    producer = null;
    console.log('[DLQ] Producer disconnected');
  }
}
