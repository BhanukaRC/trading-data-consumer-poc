import { publishMarketDlq, publishReconciliationDlq } from './dlq-producer.js';
import {
  initPartitionTracking,
  addOffsetToCompleted,
  commitOffsetsInOrder,
} from './offset-handler.js';
import {
  initReconciliationPartitionTracking,
  markReconciliationOffsetsReady,
  commitReconciliationOffsetsInOrder,
} from './reconciliation-offset-handler.js';

export async function skipMarketOffsetWithDlq(
  partition: number,
  offset: string,
  reason: string,
  errorMessage?: string,
  valuePreview?: string
): Promise<void> {

  initPartitionTracking(partition);

  try {
    await publishMarketDlq({
      sourceTopic: 'market',
      partition,
      offset,
      reason,
      errorMessage,
      valuePreview,
      detectedAt: new Date().toISOString(),
    });
  } catch (dlqError) {
    console.error(
      `[MARKET] [DLQ] Failed to publish dead-letter for offset ${offset}:`,
      dlqError
    );
  }

  addOffsetToCompleted(partition, offset);
  
  try {
    await commitOffsetsInOrder(partition);
  } catch (commitError) {
    console.error(`[MARKET] Error committing offsets for partition ${partition}:`, commitError);
  }
}

export async function skipReconciliationOffsetWithDlq(
  partition: number,
  offset: string,
  reason: string,
  errorMessage?: string,
  valuePreview?: string
): Promise<void> {

  initReconciliationPartitionTracking(partition);

  try {
    await publishReconciliationDlq({
      sourceTopic: 'reconciliation',
      partition,
      offset,
      reason,
      errorMessage,
      valuePreview,
      detectedAt: new Date().toISOString(),
    });
  } catch (dlqError) {
    console.error(
      `[RECONCILIATION] [DLQ] Failed to publish dead-letter for offset ${offset}:`,
      dlqError
    );
  }

  markReconciliationOffsetsReady(partition, [offset]);
  
  try {
    await commitReconciliationOffsetsInOrder(partition);
  } catch (commitError) {
    console.error(
      `[RECONCILIATION] Error committing offsets for partition ${partition}:`,
      commitError
    );
  }
}
