import { Readable } from 'stream';
import { RawMarketMessage, RawTradeMessage } from './types';

export interface StreamConfig {
    tradesPerSecond: number;
    tradesPerMarketMessage: number;
    outOfOrderProbability: number; 
}

export class StreamGenerator extends Readable {
    private tradeCount = 0;
    private currentMarketPeriod: { startTime: Date; endTime: Date } | null = null;
    private previousMarketPeriod: { startTime: Date; endTime: Date } | null = null;
    private pendingOutOfOrderTrades: Array<{ message: RawTradeMessage; shouldSendAfter: number }> = [];
    private isGenerating = false;
    private intervalId: NodeJS.Timeout | null = null;
    private outOfOrderIntervalId: NodeJS.Timeout | null = null;

    constructor(private config: StreamConfig) {
        super({ objectMode: false });
    }

    _read() {
        if (!this.isGenerating) {
            this.isGenerating = true;
            this.startGeneration();
        }
    }

    _destroy(error: Error | null, callback: (error: Error | null) => void) {
        if (this.intervalId) {
            clearInterval(this.intervalId);
        }
        if (this.outOfOrderIntervalId) {
            clearInterval(this.outOfOrderIntervalId);
        }
        callback(error);
    }

    private startGeneration() {
        const intervalMs = 1000 / this.config.tradesPerSecond;
        
        // Generate messages at the configured rate
        this.intervalId = setInterval(() => {
            try {
                // Check if we need to generate a market message
                if (this.tradeCount >= this.config.tradesPerMarketMessage) {
                    this.generateMarketMessage();
                    this.tradeCount = 0;
                } else {
                    this.generateTradeMessage();
                    this.tradeCount++;
                }
            } catch (error) {
                this.destroy(error as Error);
            }
        }, intervalMs);

        // Process out-of-order trades periodically
        this.outOfOrderIntervalId = setInterval(() => {
            this.processOutOfOrderTrades();
        }, 500); // Check every 500ms
    }

    private generateTradeMessage() {
        const now = new Date();
        const tradeMessage: RawTradeMessage = {
            messageType: 'trades',
            tradeType: Math.random() > 0.5 ? 'BUY' : 'SELL',
            volume: (Math.random() * 1000 + 100).toFixed(2),
            time: now.toISOString(),
        };

        // Check if this should be an out-of-order trade
        if (this.previousMarketPeriod && Math.random() < this.config.outOfOrderProbability) {
            // Generate a trade with a timestamp within the previous market period
            const periodStart = this.previousMarketPeriod.startTime.getTime();
            const periodEnd = this.previousMarketPeriod.endTime.getTime();
            const timeInPreviousPeriod = new Date(
                periodStart + Math.random() * (periodEnd - periodStart)
            );
            tradeMessage.time = timeInPreviousPeriod.toISOString();
            
            // Schedule this to be sent after the current market message (with some delay)
            // This simulates a trade that arrives late
            const delayAfterMarket = Math.random() * 4000 + 500; // 500-4500ms after market message
            this.pendingOutOfOrderTrades.push({
                message: tradeMessage,
                shouldSendAfter: Date.now() + delayAfterMarket,
            });
        } else {
            // Send normal trade message
            this.sendMessage(tradeMessage);
        }
    }

    private generateMarketMessage() {
        const now = new Date();
        const startTime = this.currentMarketPeriod?.endTime || now;
        const endTime = new Date(startTime.getTime() + 3000); // 3 second market period

        const marketMessage: RawMarketMessage = {
            messageType: 'market',
            buyPrice: (Math.random() * 100 + 50).toFixed(2),
            sellPrice: (Math.random() * 100 + 50).toFixed(2),
            startTime: startTime.toISOString(),
            endTime: endTime.toISOString(),
        };

        this.sendMessage(marketMessage);
        
        if (this.currentMarketPeriod !== null) {
            this.previousMarketPeriod = this.currentMarketPeriod;
        }
        this.currentMarketPeriod = { startTime, endTime };
    }

    private processOutOfOrderTrades() {
        const now = Date.now();
        const readyTrades = this.pendingOutOfOrderTrades.filter(
            trade => now >= trade.shouldSendAfter
        );

        for (const trade of readyTrades) {
            this.sendMessage(trade.message);
            const index = this.pendingOutOfOrderTrades.indexOf(trade);
            if (index > -1) {
                this.pendingOutOfOrderTrades.splice(index, 1);
            }
        }
    }

    private sendMessage(message: RawMarketMessage | RawTradeMessage) {
        const sseFormat = `data: ${JSON.stringify(message)}\n\n`;
        this.push(Buffer.from(sseFormat, 'utf-8'));
    }
}

export function createStreamGenerator(config: StreamConfig): StreamGenerator {
    return new StreamGenerator(config);
}
