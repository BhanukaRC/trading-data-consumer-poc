import type { Producer } from 'kafkajs';

export type DlqTestConfig = {
    enabled: boolean;
    intervalMs: number;
};

const MARKET_SCENARIOS = [
    'market_empty_value',
    'market_non_json',
    'market_invalid_nan_price',
    'market_invalid_iso_dates',
    'market_wrong_message_type',
] as const;

const RECONCILIATION_SCENARIOS = [
    'reconciliation_empty_value',
    'reconciliation_non_json',
    'reconciliation_invalid_shape',
] as const;

type Scenario = (typeof MARKET_SCENARIOS)[number] | (typeof RECONCILIATION_SCENARIOS)[number];

const ALL_SCENARIOS: Scenario[] = [...MARKET_SCENARIOS, ...RECONCILIATION_SCENARIOS];

let scenarioIndex = 0;

let dlqTestInterval: NodeJS.Timeout | null = null;

export function stopDlqTestProducer(): void {
    if (dlqTestInterval !== null) {
        clearInterval(dlqTestInterval);
        dlqTestInterval = null;
    }
}

/**
 * Periodically publishes intentionally bad records so downstream consumers can route them to DLQ.
 */
export function startDlqTestProducer(producer: Producer, config: DlqTestConfig): void {
    stopDlqTestProducer();

    if (!config.enabled) {
        console.log('[DLQ-TEST] Disabled (set DLQ_TEST_ENABLED=true to enable)');
        return;
    }

    console.log(`[DLQ-TEST] Enabled: every ${config.intervalMs}ms rotating through ${ALL_SCENARIOS.length} bad-message shapes`);

    dlqTestInterval = setInterval(() => {
        const scenario = ALL_SCENARIOS[scenarioIndex % ALL_SCENARIOS.length]!;
        scenarioIndex += 1;

        void sendDlqScenario(producer, scenario).catch((err) => {
            console.error(`[DLQ-TEST] Send failed (${scenario}):`, err);
        });
    }, config.intervalMs);
}

async function sendDlqScenario(producer: Producer, scenario: Scenario): Promise<void> {
    switch (scenario) {
        case 'market_empty_value':
            await producer.send({
                topic: 'market',
                messages: [{ value: null, timestamp: Date.now().toString() }],
            });
            break;

        case 'market_non_json':
            await producer.send({
                topic: 'market',
                messages: [
                    {
                        value: 'not-json at all {garbage',
                        timestamp: Date.now().toString(),
                    },
                ],
            });
            break;

        case 'market_invalid_nan_price':
            await producer.send({
                topic: 'market',
                messages: [
                    {
                        value: JSON.stringify({
                            messageType: 'market',
                            buyPrice: 'not-a-number',
                            sellPrice: '99.00',
                            startTime: new Date().toISOString(),
                            endTime: new Date(Date.now() + 3000).toISOString(),
                        }),
                        timestamp: Date.now().toString(),
                    },
                ],
            });
            break;

        case 'market_invalid_iso_dates':
            await producer.send({
                topic: 'market',
                messages: [
                    {
                        value: JSON.stringify({
                            messageType: 'market',
                            buyPrice: '50.00',
                            sellPrice: '51.00',
                            startTime: 'not-an-iso-date',
                            endTime: 'also-bad',
                        }),
                        timestamp: Date.now().toString(),
                    },
                ],
            });
            break;

        case 'market_wrong_message_type':
            await producer.send({
                topic: 'market',
                messages: [
                    {
                        value: JSON.stringify({
                            messageType: 'trades',
                            tradeType: 'BUY',
                            volume: '100.00',
                            time: new Date().toISOString(),
                        }),
                        timestamp: Date.now().toString(),
                    },
                ],
            });
            break;

        case 'reconciliation_empty_value':
            await producer.send({
                topic: 'reconciliation',
                messages: [{ value: null, timestamp: Date.now().toString() }],
            });
            break;

        case 'reconciliation_non_json':
            await producer.send({
                topic: 'reconciliation',
                messages: [
                    {
                        value: '<<< not json >>>',
                        timestamp: Date.now().toString(),
                    },
                ],
            });
            break;

        case 'reconciliation_invalid_shape':
            await producer.send({
                topic: 'reconciliation',
                messages: [
                    {
                        value: JSON.stringify({
                            tradeTime: 'not-iso',
                            detectedAt: new Date().toISOString(),
                        }),
                        timestamp: Date.now().toString(),
                    },
                ],
            });
            break;

    }

    console.warn(`[DLQ-TEST] Published scenario=${scenario} (expect calculation-service DLQ + commit)`);
}
