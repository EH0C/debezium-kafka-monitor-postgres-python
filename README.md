# Kafka CDC Monitor

Real-time Change Data Capture (CDC) pipeline using Debezium, Kafka, and PostgreSQL.
Monitors the `orders` table and alerts when an order has an amount of 0.

## Stack

| Service | Image | Port |
|---|---|---|
| PostgreSQL | postgres:16 | 5432 |
| Kafka (KRaft) | confluentinc/cp-kafka:7.7.0 | 9092 |
| Kafka Connect | debezium/connect:2.5 | 8083 |
| Monitor | python:3.12-slim | — |

## Prerequisites

- Docker Desktop

## Getting Started

**1. Start the stack**
```bash
docker compose up -d
```

**2. Wait for all containers to be healthy**
```bash
docker ps
```

**3. Create the database schema**
```bash
docker exec cdc-postgres psql -U admin -d yourdb -c "
CREATE TABLE IF NOT EXISTS customers (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
  id SERIAL PRIMARY KEY,
  customer_id INT REFERENCES customers(id),
  product TEXT NOT NULL,
  amount NUMERIC(10,2) NOT NULL
);
INSERT INTO customers (name) VALUES ('Test Customer');
"
```

**4. Register the Debezium connector**
```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @connector.json
```

**5. Watch for alerts**
```bash
docker logs -f cdc-monitor
```

## How It Works

```
PostgreSQL (INSERT/UPDATE)
  → Debezium (CDC via pgoutput)
    → Kafka topic: myserver.public.orders
      → Python monitor (alerts when amount = 0)
```

- Debezium streams every INSERT and UPDATE from `public.orders` and `public.customers` to Kafka
- The Python monitor container listens to the `myserver.public.orders` topic 24/7
- Any order with `amount = 0` triggers an alert in the logs
- The monitor restarts automatically if it crashes

## Useful Commands

```bash
# Check all containers are running
docker ps

# Check connector status
curl -s http://localhost:8083/connectors/postgres-connector/status

# View live alerts
docker logs -f kafkaproject-monitor-1

# Insert a test zero-amount order
docker exec kafkaproject-postgres-1 psql -U admin -d yourdb \
  -c "INSERT INTO orders (customer_id, product, amount) VALUES (1, 'test', 0.00);"

# List Kafka topics
docker exec kafkaproject-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
```

## Project Structure

```
kafkaproject/
├── docker-compose.yml     # Full stack definition
├── connector.json         # Debezium PostgreSQL connector config
├── monitor_orders.py      # Python Kafka consumer — alerts on amount = 0
└── README.md
```
