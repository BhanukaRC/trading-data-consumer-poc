import { getMarketsCollection, getPnLsCollection } from './db.js';
import { MarketDocument, PnLDocument, ReconciliationMessage } from './types.js';
import { getTradesForPeriod, retryWithBackoff } from './grpc-client.js';
import { calculatePnL } from './pnl-calculation.js';
import Decimal from 'decimal.js';
import { addReconciliationOffset, markReconciliationOffsetsReady, commitReconciliationOffsetsInOrder } from './reconciliation-offset-handler.js';

// In-memory set of reconciliation timestamps (trade times that need recalculation)
const reconciliationTimestamps = new Set<string>();
// Map: timestamp -> list of offsets that contributed to this reconciliation
const timestampToOffsets = new Map<string, Array<{ partition: number; offset: string }>>();

// Add a reconciliation timestamp with its offset
export function addReconciliationTimestamp(tradeTime: string, partition: number, offset: string): void {
  reconciliationTimestamps.add(tradeTime);
  
  // Track which offsets contributed to this timestamp
  if (!timestampToOffsets.has(tradeTime)) {
    timestampToOffsets.set(tradeTime, []);
  }
  timestampToOffsets.get(tradeTime)!.push({ partition, offset });
  
  // Track offset as pending
  addReconciliationOffset(partition, offset);
}

// Remove reconciliation timestamps for a given market period
export function removeReconciliationTimestampsForPeriod(startTime: Date, endTime: Date): void {
  const startTimeStr = startTime.toISOString();
  const endTimeStr = endTime.toISOString();
  
  let removed = 0;
  reconciliationTimestamps.forEach((timestamp) => {
    if (timestamp >= startTimeStr && timestamp <= endTimeStr) {
      reconciliationTimestamps.delete(timestamp);
      removed++;
    }
  });

}

// Get all reconciliation timestamps
export function getReconciliationTimestamps(): string[] {
  return Array.from(reconciliationTimestamps);
}

type MarketWithTimestamps = {
  market: MarketDocument;
  timestamps: string[];
}

// Find market periods that contain reconciliation timestamps
// Returns market periods with their timestamps and unmatched timestamps
type ReconciliationResult = {
  marketPeriods: MarketWithTimestamps[];
  unmatchedTimestamps: string[];
};

export async function findMarketPeriodsForReconciliation(): Promise<ReconciliationResult> {
  if (reconciliationTimestamps.size === 0) {
    return { marketPeriods: [], unmatchedTimestamps: [] };
  }

  const timestamps = Array.from(reconciliationTimestamps);
  const markets = getMarketsCollection();

  // Get time range covering all timestamps for efficient querying
  const times = timestamps.map(ts => new Date(ts)).sort((a, b) => a.getTime() - b.getTime());
  const minTime = times[0]!;
  const maxTime = times[times.length - 1]!;

  // Query all markets that might contain any of the timestamps
  // Markets where: (startTime <= maxTime AND endTime >= minTime)
  const candidateMarkets = await markets
    .find({
      startTime: { $lte: maxTime },
      endTime: { $gte: minTime },
    })
    .toArray();

  const marketPeriods: MarketWithTimestamps[] = [];

  // For each market, find which timestamps fall within it
  for (const market of candidateMarkets) {
    const marketStart = market.startTime.getTime();
    const marketEnd = market.endTime.getTime();
    
    // Find all timestamps that fall within this market period
    const timestampsInMarket: string[] = [];
    for (const timestamp of timestamps) {
      const tradeTime = new Date(timestamp).getTime();
      if (tradeTime >= marketStart && tradeTime <= marketEnd) {
        timestampsInMarket.push(timestamp);
      }
    }

    // If this market contains any reconciliation timestamps, add it
    if (timestampsInMarket.length > 0) {
      marketPeriods.push({
        market,
        timestamps: timestampsInMarket, 
      });
    }
  }

  // Find which timestamps were NOT matched to any market
  const matchedTimestamps = new Set<string>();
  for (const { timestamps: matched } of marketPeriods) {
    for (const ts of matched) {
      matchedTimestamps.add(ts);
    }
  }
  
  const unmatchedTimestamps = timestamps.filter(ts => !matchedTimestamps.has(ts));

  return { marketPeriods, unmatchedTimestamps };
}

// Retrigger PnL calculation for a market period
// Returns offsets that should be committed after successful DB write
export async function retriggerPnLCalculation(market: MarketDocument, timestamps: string[]): Promise<Array<{ partition: number; offset: string }>> {
  const startTime = market.startTime;
  const endTime = market.endTime;
  const buyPrice = new Decimal(market.buyPrice);
  const sellPrice = new Decimal(market.sellPrice);

  try {
    // Get trades for this period (will include the out-of-order trade now)
    const trades = await retryWithBackoff(() =>
      getTradesForPeriod(startTime, endTime)
    );

    // Recalculate PnL
    const pnlResult = calculatePnL(buyPrice, sellPrice, trades);

    const pnlDoc: PnLDocument = {
      marketStartTime: startTime,
      marketEndTime: endTime,
      buyPrice: market.buyPrice,
      sellPrice: market.sellPrice,
      totalBuyVolume: pnlResult.totalBuyVolume.toString(),
      totalSellVolume: pnlResult.totalSellVolume.toString(),
      totalBuyCost: pnlResult.totalBuyCost.toString(),
      totalSellRevenue: pnlResult.totalSellRevenue.toString(),
      totalFees: pnlResult.totalFees.toString(),
      pnl: pnlResult.pnl.toString(),
      createdAt: new Date(),
    };

    // Update PnL in database (replace existing)
    const pnlsCollection = getPnLsCollection();
    await pnlsCollection.replaceOne(
      {
        marketStartTime: startTime,
        marketEndTime: endTime,
      },
      pnlDoc,
      { upsert: true }
    );

    console.log(
      `[RECONCILIATION] Updated PnL for ${startTime.toISOString()} - ${endTime.toISOString()}: ${pnlResult.pnl.toString()} € (Buy: ${pnlResult.totalBuyVolume.toString()} MWh, Sell: ${pnlResult.totalSellVolume.toString()} MWh)`
    );

    // Collect all offsets that contributed to this reconciliation
    const offsetsToCommit: Array<{ partition: number; offset: string }> = [];
    for (const timestamp of timestamps) {
      const offsets = timestampToOffsets.get(timestamp);
      if (offsets) {
        offsetsToCommit.push(...offsets);
        timestampToOffsets.delete(timestamp); // Clean up offset mapping
      }
      // Remove timestamp from reconciliation set only after successful DB write
      reconciliationTimestamps.delete(timestamp);
    }
    return offsetsToCommit;
  } catch (error) {
    console.error(`[RECONCILIATION] Error retriggering PnL calculation:`, error);
    throw error;
  }
}

// Process all pending reconciliations
export async function processPendingReconciliations(): Promise<void> {
  const pendingCount = reconciliationTimestamps.size;
  if (pendingCount === 0) {
    return;
  }

  const { marketPeriods, unmatchedTimestamps } = await findMarketPeriodsForReconciliation();

  // Handle unmatched timestamps: commit their offsets immediately since they have no matching market
  if (unmatchedTimestamps.length > 0) {
    console.log(`[RECONCILIATION] Found ${unmatchedTimestamps.length} unmatched timestamps (no market in DB), committing offsets`);
    // Group offsets by partition for unmatched timestamps
    const unmatchedOffsetsByPartition = new Map<number, string[]>();
    for (const timestamp of unmatchedTimestamps) {
      const offsets = timestampToOffsets.get(timestamp);
      if (offsets) {
        for (const { partition, offset } of offsets) {
          if (!unmatchedOffsetsByPartition.has(partition)) {
            unmatchedOffsetsByPartition.set(partition, []);
          }
          unmatchedOffsetsByPartition.get(partition)!.push(offset);
        }
      }
      // Remove timestamp from reconciliation set
      reconciliationTimestamps.delete(timestamp);
      timestampToOffsets.delete(timestamp);
    }

    // Mark offsets as ready to commit and commit them
    for (const [partition, offsets] of unmatchedOffsetsByPartition) {
      const uniqueOffsets = Array.from(new Set(offsets)); // Remove duplicates
      markReconciliationOffsetsReady(partition, uniqueOffsets);
      await commitReconciliationOffsetsInOrder(partition).catch((error) => {
        console.error(`[RECONCILIATION] Error committing unmatched offsets for partition ${partition}:`, error);
      });
    }
  }

  if (marketPeriods.length === 0) {
    return;
  }

  // Process each market period
  for (const { market, timestamps } of marketPeriods) {
    try {
      const offsetsToCommit = await retriggerPnLCalculation(market, timestamps);
      
      // Group offsets by partition for efficient committing
      const offsetsByPartition = new Map<number, string[]>();
      for (const { partition, offset } of offsetsToCommit) {
        if (!offsetsByPartition.has(partition)) {
          offsetsByPartition.set(partition, []);
        }
        offsetsByPartition.get(partition)!.push(offset);
      }

      // Mark offsets as ready to commit and commit them
      for (const [partition, offsets] of offsetsByPartition) {
        markReconciliationOffsetsReady(partition, offsets);
        await commitReconciliationOffsetsInOrder(partition).catch((error) => {
          console.error(`[RECONCILIATION] Error committing offsets for partition ${partition}:`, error);
        });
      }
    } catch (error) {
      console.error(
        `[RECONCILIATION] Failed to process reconciliation for market ${market.startTime.toISOString()} - ${market.endTime.toISOString()}:`,
        error
      );
      // Continue with other reconciliations even if one fails
    }
  }
  
}
