import { Consumer } from 'kafkajs';

let reconciliationConsumer: Consumer | null = null;

// Track reconciliation offsets per partition
// Map: partition -> Set of offsets waiting for reconciliation
const pendingReconciliationOffsets = new Map<number, Set<string>>();
// Map: partition -> Set of offsets ready to commit (after successful DB write)
const readyToCommitOffsets = new Map<number, Set<string>>();
// Map: partition -> last committed offset
const lastCommittedReconciliationOffset = new Map<number, string>();

export function setReconciliationConsumer(consumer: Consumer): void {
  reconciliationConsumer = consumer;
}

export function initReconciliationPartitionTracking(partition: number): void {
  if (!pendingReconciliationOffsets.has(partition)) {
    pendingReconciliationOffsets.set(partition, new Set());
    readyToCommitOffsets.set(partition, new Set());
  }
}

export function addReconciliationOffset(partition: number, offset: string): void {
  initReconciliationPartitionTracking(partition);
  pendingReconciliationOffsets.get(partition)!.add(offset);
}

// Mark reconciliation offsets as ready to commit (after successful DB write)
export function markReconciliationOffsetsReady(partition: number, offsets: string[]): void {
  initReconciliationPartitionTracking(partition);
  const readySet = readyToCommitOffsets.get(partition)!;
  offsets.forEach(offset => {
    readySet.add(offset);
    // Remove from pending since it's now ready
    pendingReconciliationOffsets.get(partition)?.delete(offset);
  });
}

// Commit reconciliation offsets in order
// Optimized to commit consecutive offsets in batches (commit highest consecutive offset)
export async function commitReconciliationOffsetsInOrder(partition: number): Promise<void> {
  const ready = readyToCommitOffsets.get(partition);
  const lastCommitted = lastCommittedReconciliationOffset.get(partition);

  if (!ready || ready.size === 0 || !reconciliationConsumer) {
    return;
  }

  // Find the starting point for consecutive offsets
  let startOffset: string | null = null;
  if (!lastCommitted) {
    const sorted = Array.from(ready).sort((a, b) => {
      const aBig = BigInt(a);
      const bBig = BigInt(b);
      return aBig < bBig ? -1 : aBig > bBig ? 1 : 0;
    });
    startOffset = sorted[0]!;
  } else {
    const expectedNext = (BigInt(lastCommitted) + BigInt(1)).toString();
    if (ready.has(expectedNext)) {
      startOffset = expectedNext;
    } else {
      return; // No consecutive offsets to commit
    }
  }

  // Find the highest consecutive offset starting from startOffset
  let highestConsecutive = BigInt(startOffset);
  let current = BigInt(startOffset);
  
  while (ready.has(current.toString())) {
    highestConsecutive = current;
    current = current + BigInt(1);
  }

  // Commit the highest consecutive offset (this implicitly commits all lower offsets)
  try {
    await reconciliationConsumer.commitOffsets([
      {
        topic: 'reconciliation',
        partition,
        offset: (highestConsecutive + BigInt(1)).toString(),
      },
    ]);

    // Remove all committed offsets from ready set
    let offsetToRemove = BigInt(startOffset);
    while (offsetToRemove <= highestConsecutive) {
      ready.delete(offsetToRemove.toString());
      offsetToRemove = offsetToRemove + BigInt(1);
    }

    lastCommittedReconciliationOffset.set(partition, highestConsecutive.toString());
    
    const count = Number(highestConsecutive - BigInt(startOffset) + BigInt(1));
    // Only log batch commits to reduce log noise
    if (count > 1) {
      console.log(`[RECONCILIATION] Committed ${count} consecutive offsets (${startOffset} to ${highestConsecutive.toString()}) for partition ${partition}`);
    }

    // Recursively commit any remaining consecutive offsets
    await commitReconciliationOffsetsInOrder(partition);
  } catch (error) {
    console.error(`[RECONCILIATION] [Partition ${partition}] Failed to commit offsets ${startOffset} to ${highestConsecutive.toString()}:`, error);
  }
}
