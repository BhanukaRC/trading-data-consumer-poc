# Kafka Producer

Generates trade and market messages and publishes them to Kafka topics.

## Configuration

Environment variables (defaults shown):
- `TRADES_PER_SECOND`: 100
- `TRADES_PER_MARKET_MESSAGE`: 300
- `OUT_OF_ORDER_PROBABILITY`: 0.005

Configure in `docker-compose.yml` or via environment variables.
