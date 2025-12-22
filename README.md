# Trading Data Consumer PoC

A proof-of-concept demonstrating high-performance Kafka usage for processing real-time trading data. This system consumes trade and market messages from Kafka, calculates PnL (Profit and Loss) in real-time, handles out-of-order trades with reconciliation, and provides a web dashboard for monitoring.

## Overview

This PoC showcases:
- High-throughput Kafka message processing with manual offset management
- Real-time PnL calculations
- Out-of-order trade reconciliation mechanism
- Scalable microservices architecture
- In-memory buffering and persistence strategies
- High-load processing capabilities

## Quick Start

1. Clone this repository
2. Run `npm start` to start all services
3. Run `npm run kafka:setup` to create Kafka topics
4. Access the frontend at `http://localhost:3000`

## Services

- **kafka-producer**: Generates and publishes trade and market messages to Kafka, including configurable out-of-order trades
- **trade-memory-service**: Maintains in-memory buffer of recent trades and detects out-of-order trades
- **trade-persistence-service**: Persists trades to MongoDB with batch processing
- **calculation-service**: Calculates PnL based on trades and market data, handles reconciliation for out-of-order trades
- **frontend-service**: Provides REST API with Server-Sent Events (SSE) for real-time updates
- **frontend**: Web dashboard for displaying real-time metrics

## Key Features

### Out-of-Order Trade Reconciliation
- Detects trades that arrive after their corresponding market period
- Automatically triggers PnL recalculation when out-of-order trades are detected
- Handles unmatched timestamps gracefully by committing offsets to prevent lag

### Manual Offset Management
- Manual offset commits after successful database writes ensure data consistency
- Batch offset commits for consecutive offsets to optimize throughput
- Prevents data loss and ensures exactly-once processing semantics

### High-Load Processing
- Designed to handle high message throughput
- In-memory buffering for fast lookups
- Efficient batch processing and offset management

## Configuration

The kafka-producer can be configured via environment variables in `docker-compose.yml`:
- `TRADES_PER_SECOND`: Trade message generation rate (default: 100)
- `TRADES_PER_MARKET_MESSAGE`: Number of trades before a market message (default: 300)
- `OUT_OF_ORDER_PROBABILITY`: Probability of out-of-order trades (default: 0.005)

## Architecture Highlights

- **Kafka Topics**: `trades`, `market`, `reconciliation`
- **Database**: MongoDB with replica set for transactions
- **Communication**: gRPC for inter-service communication, SSE for frontend updates
- **Offset Strategy**: Manual commits after successful DB writes for data consistency
