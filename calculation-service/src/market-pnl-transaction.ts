import { getClient, getMarketsCollection, getPnLsCollection } from './db.js';
import { isMongoDbError, MarketDocument, PnLDocument } from './types.js';
import { addMarketToBuffer } from './market-buffer.js';

const MONGODB_DUPLICATE_KEY_ERROR_CODE = 11000;

// Write market and PnL atomically using transaction
// Returns true if already processed (idempotency), false if successfully processed
export async function writeMarketAndPnL(
  marketDoc: MarketDocument,
  pnlDoc: PnLDocument
): Promise<boolean> {
  const markets = getMarketsCollection();
  const pnlsCollection = getPnLsCollection();
  const client = getClient();
  const session = client.startSession();

  try {
    await session.withTransaction(async () => {
      await Promise.all([
        markets.insertOne(marketDoc, { session }),
        pnlsCollection.insertOne(pnlDoc, { session })
      ]);
    });

    return false; // Successfully processed
  } catch (transactionError: unknown) {
    if (isMongoDbError(transactionError) && transactionError.code === MONGODB_DUPLICATE_KEY_ERROR_CODE) {
      return true; // Already processed
    }
    console.error(`[MARKET] Transaction failed for ${marketDoc.startTime.toISOString()} - ${marketDoc.endTime.toISOString()}:`, transactionError);
    throw transactionError;
  }
  finally {
    await session.endSession();
  }
}
