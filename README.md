# 📖 Real-Time Fraud Detection Engine

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Docker](https://img.shields.io/badge/docker-enabled-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green.svg)
![Kafka](https://img.shields.io/badge/Kafka-event--streaming-orange.svg)
![Redis](https://img.shields.io/badge/Redis-hot--state-red.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-ledger-blue.svg)
![Tests](https://img.shields.io/badge/tests-18%20passing-brightgreen.svg)

A production-inspired real-time fraud detection and payment authorization engine built with **FastAPI**, **Kafka**, **Redis**, **PostgreSQL**, and **Docker Compose**.

The project simulates how a fintech backend might process financial transactions, apply fraud intelligence, and persist decisions into an immutable ledger.

It focuses on:

* Sub-500ms hot-path design principles
* Effectively-once processing semantics
* Redis-backed idempotency protection
* Event-driven transaction processing with Kafka
* Receiver-centric fraud detection for mule swarm behaviour
* APP scam panic scoring
* Structured JSON observability logs
* PostgreSQL ledger persistence
* Dockerised local execution

---

## 🚀 Quick Start

The full system runs locally through Docker Compose.

### Prerequisites

* Docker
* Docker Compose

### Run the Full System

```bash
git clone https://github.com/Izumizon/realtime-fraud-engine.git
cd realtime-fraud-engine

docker compose up --build
```

This starts:

* PostgreSQL ledger
* Redis hot-state cache
* Kafka event broker
* Zookeeper
* FastAPI API Gateway
* Fraud Engine Kafka Consumer
* Traffic Simulator

### Verify API Health

In a second terminal:

```bash
curl http://localhost:8000/health
```

Expected response:

```json
{
  "status": "healthy"
}
```

### View Fraud Engine Logs

To view only the fraud engine logs:

```bash
docker compose logs -f fraud_engine
```

You should see structured JSON decision logs like:

```json
{
  "event": "transaction_decision",
  "service": "fraud_engine",
  "trace_id": "12cfb8cb-a53b-4bf1-ae77-72f6b635c112",
  "transaction_id": "3cdf3e66-3f9e-41f7-80ea-f2e5a43d8078",
  "user_id": "user_1012",
  "merchant_id": "m-1102",
  "amount": 10627,
  "currency": "EUR",
  "status": "APPROVED",
  "risk_score": 0,
  "reasons": [],
  "redis_eval_ms": 0.513,
  "sender_velocity_count": 1,
  "receiver_unique_sender_count": 1
}
```

---

## ✅ Current Features

### Core Infrastructure

* Docker Compose environment
* PostgreSQL transaction ledger
* Redis hot-state cache
* Kafka event broker
* FastAPI API Gateway
* Async Kafka consumer
* Traffic simulator service

### Fraud Detection

* 0–100 dynamic risk scoring
* APP scam panic detection
* New payee risk scoring
* Recent password reset risk scoring
* Fast transaction completion risk scoring
* Sender velocity detection
* Receiver swarm detection for mule-network behaviour

### Reliability & Correctness

* Redis-backed API idempotency state machine
* Duplicate request protection through `Idempotency-Key`
* Conflicting duplicate payload rejection
* PostgreSQL primary-key protection against duplicate ledger writes
* Manual Kafka offset commits after successful processing
* Replay-safe consumer behaviour

### Observability

* End-to-end `trace_id` propagation
* Structured JSON transaction decision logs
* Redis evaluation latency logging
* Kafka offset commit logging
* Fraud reason logging

### Testing

Current test suite:

```bash
python -m pytest
```

Current status:

```text
18 passed
```

Test coverage includes:

* Static fraud scoring
* Risk routing boundaries
* Redis sender velocity detection
* Redis receiver swarm detection
* Duplicate transaction handling
* API idempotency behaviour
* Missing idempotency header rejection
* Conflicting idempotency payload rejection

---

# Architecture & Engineering RFC

## 1. System Overview & Philosophy

This project simulates a production-grade fintech payment authorization system inspired by modern card payment processors.

The architecture minimises known distributed failure modes under defined constraints, prioritising:

* Strict P99 latency target for the hot path
* Effectively-once processing semantics under bounded assumptions
* Risk-tiered degradation under partial system failure
* Clear separation between decisioning and analytics
* High observability and auditability for financial workflows

---

## 1.1 Fraud Decision Philosophy

During uncertainty or degraded system state, the engine prioritises preventing financial loss over minimising false positives.

The system accepts temporary customer friction when risk signals are high or infrastructure health is degraded.

---

## 1.2 Consistency Guarantees by Subsystem

### PostgreSQL Ledger

Strong consistency through ACID transactions.

PostgreSQL acts as the durable system of record for evaluated transactions.

### Kafka Event Stream

Eventual consistency.

Kafka acts as the asynchronous transport layer for transaction events.

### Redis Hot State

Atomic per-key operations using Redis Lua scripts.

Redis stores short-lived fraud intelligence state such as idempotency keys, velocity windows, and receiver swarm windows.

### Merchant / Receiver Risk

Eventually consistent.

Receiver risk is inferred from short-lived Redis windows and can later be expanded into longer-term merchant risk profiles.

---

## 1.3 System Boundaries

The current implementation does not include:

* Real-time ML model training
* Distributed consensus algorithms
* Cross-region active-active replication
* Blockchain or exotic consistency models
* Full payment settlement
* Real bank integrations
* Production authentication / mTLS
* Grafana dashboards
* GitHub Actions CI/CD

These are intentionally out of scope for the current portfolio version.

---

## 2. System Architecture & Service Ownership

## `api_gateway` — FastAPI Hot Path

The API Gateway owns:

* HTTP request validation
* `Idempotency-Key` enforcement
* Redis idempotency state transitions
* Kafka event publishing
* Health check endpoint

It does not own long-term fraud analytics or ledger persistence.

### Inbound API Contract

Endpoint:

```http
POST /api/v1/transactions
```

Required headers:

```http
Idempotency-Key: <uuid>
Authorization: Bearer <token>
```

Optional header:

```http
X-Trace-Id: <trace-id>
```

Example payload:

```json
{
  "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "user_1001",
  "merchant_id": "m-9921",
  "amount": 1050,
  "currency": "GBP",
  "device_ip": "192.168.1.1",
  "behavioral_metadata": {
    "time_to_complete_ms": 1200,
    "is_new_payee": true,
    "password_reset_24h": false
  }
}
```

Financial amounts are represented as integer minor units.

Example:

```text
£10.50 = 1050
```

This avoids floating-point precision errors.

---

## `fraud_engine` — Kafka Consumer / Intelligence Layer

The fraud engine owns:

* Kafka message consumption
* Static APP scam scoring
* Redis dynamic fraud evaluation
* Risk score calculation
* Transaction routing
* Structured decision logging
* PostgreSQL ledger writes

It does not own HTTP request handling.

---

## `traffic_simulator` — Chaos & Load Generator

The traffic simulator owns:

* Normal transaction simulation
* APP scam panic simulation
* Randomised user and merchant activity
* Continuous transaction stream generation

It treats the system as an external client and writes events into Kafka.

---

# 3. Fraud Scoring Model

The engine uses a 0–100 risk score.

## Decision Bands

| Score Range | Decision       |
| ----------- | -------------- |
| 0–39        | APPROVED       |
| 40–69       | STEP-UP_REVIEW |
| 70–100      | DECLINED       |

---

## 3.1 Static APP Scam Scoring

Static scoring is deterministic and does not require network calls.

Risk signals include:

| Signal                                   | Risk Penalty |
| ---------------------------------------- | ------------ |
| New payee                                | +20          |
| Password reset in last 24h               | +30          |
| Transaction completed in under 3 seconds | +40          |

These signals model APP scam behaviour where a legitimate user may be manipulated into quickly sending money to a new recipient.

---

## 3.2 Redis Dynamic Fraud Intelligence

Redis is used for short-lived, low-latency fraud state.

### Sender Velocity

Redis key:

```text
vel:user:{user_id}
```

Purpose:

Detects whether one user is attempting too many transactions inside a 10-minute sliding window.

### Receiver Swarm Detection

Redis key:

```text
vel:merch:{merchant_id}
```

Purpose:

Detects whether many unique users are suddenly sending funds to the same merchant or receiver.

This is designed to model mule-network and micro-structuring patterns.

---

# 4. Idempotency Model

The API Gateway enforces idempotency using Redis.

This prevents duplicate transaction submission caused by:

* User double-clicking
* Client retrying after timeout
* Merchant retrying the same request
* Network failure after a successful request

## Idempotency States

| State      | Meaning                           |
| ---------- | --------------------------------- |
| PROCESSING | First request owns execution      |
| COMPLETED  | Final response cached             |
| FAILED     | Request failed and may be retried |

## Behaviour

| Scenario                        | Result                 |
| ------------------------------- | ---------------------- |
| First request with new key      | Publish to Kafka       |
| Same key + same payload         | Return cached response |
| Same key + different payload    | Return 409 Conflict    |
| Same key while still processing | Return 202 Accepted    |

State transitions are handled through Redis Lua scripts to keep operations atomic.

---

# 5. Data Architecture

## 5.1 PostgreSQL Ledger

PostgreSQL stores evaluated transaction decisions.

The ledger includes:

* transaction_id
* trace_id
* user_id
* merchant_id
* amount
* currency
* status
* risk_score
* received_at

`transaction_id` is the primary key.

Duplicate inserts are ignored using PostgreSQL conflict handling, protecting the ledger from duplicate Kafka delivery or consumer retry behaviour.

---

## 5.2 Redis Hot State

Redis stores ephemeral fraud state.

Current key patterns:

```text
idem:{id}
vel:user:{user_id}
vel:merch:{merchant_id}
```

Redis keys use TTLs so stale risk state naturally expires.

---

## 5.3 Kafka Event Stream

Current Kafka topic:

```text
payment_transactions
```

The API Gateway and traffic simulator publish transaction events into this topic.

The fraud engine consumes from this topic using a Kafka consumer group.

Future versions may split approved and declined events into separate topics.

---

# 6. Observability

The system emits structured JSON logs.

Each transaction decision includes:

* event name
* service name
* trace_id
* transaction_id
* user_id
* merchant_id
* amount
* currency
* decision status
* risk score
* fraud reasons
* Redis evaluation time
* sender velocity count
* receiver unique sender count

Example decision reasons:

```text
new_payee
recent_password_reset
panic_execution_speed
sender_velocity_exceeded
receiver_swarm_detected
```

Kafka offset commits are also logged after successful processing.

---

# 7. Testing

The project includes a pytest suite.

Run tests:

```bash
python -m pytest
```

Current status:

```text
18 passed
```

Test files:

```text
tests/test_risk_scoring.py
tests/test_redis_fraud_evaluator.py
tests/test_api_idempotency.py
```

Coverage includes:

* Static risk scoring
* Routing boundaries
* Redis fraud intelligence
* API idempotency
* Duplicate request handling
* Missing header rejection
* Conflicting payload rejection

---

# 8. Local Development Commands

## Start full system

```bash
docker compose up --build
```

## Start in detached mode

```bash
docker compose up --build -d
```

## Stop system

```bash
docker compose down
```

## Reset all data

```bash
docker compose down -v
```

## View all logs

```bash
docker compose logs -f
```

## View fraud engine logs only

```bash
docker compose logs -f fraud_engine
```

## Run tests locally

```bash
python -m pytest
```

---

# 9. Roadmap

## Completed

* FastAPI API Gateway
* Kafka transaction pipeline
* PostgreSQL ledger
* Redis fraud intelligence
* APP scam scoring
* Receiver swarm detection
* API idempotency
* Trace ID propagation
* Structured JSON logs
* Dockerised infrastructure and Python services
* Pytest suite with 18 passing tests

## Next

* Clean CI/CD pipeline with GitHub Actions
* Linting with ruff
* Static type checking with mypy
* Prometheus metrics endpoint
* Grafana dashboard
* Better traffic simulator scenarios
* Architecture diagrams
* More integration tests

---

# 10. AI Usage Disclosure

AI tools were used during this project as engineering assistants for architecture review, debugging guidance, documentation structure, and production-readiness critique.

All implementation decisions, local debugging, testing, commits, and system validation were owned by me.

The project was not treated as a copy-paste exercise. AI was used similarly to a senior engineering mentor: to challenge assumptions, identify hidden failure modes, and accelerate learning.

---

# 11. Key Learning Outcomes

This project strengthened my understanding of:

* Event-driven backend systems
* Kafka consumer behaviour
* Redis Lua scripting
* Sliding-window fraud detection
* API idempotency
* PostgreSQL ledger design
* Async Python service design
* Docker Compose orchestration
* Structured observability logs
* Distributed systems tradeoffs
