# 📖 Real-Time Fraud Detection Engine

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Docker](https://img.shields.io/badge/docker-enabled-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green.svg)
![Kafka](https://img.shields.io/badge/Kafka-event--streaming-orange.svg)
![Redis](https://img.shields.io/badge/Redis-hot--state-red.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-ledger-blue.svg)
![Tests](https://img.shields.io/badge/tests-18%20passing-brightgreen.svg)

A production-inspired real-time fraud detection and payment authorization engine built with **FastAPI**, **Kafka**, **Redis**, **PostgreSQL**, and **Docker Compose**.

The project simulates how a fintech backend might receive transactions, apply fraud intelligence, persist decisions into an immutable ledger, and expose a live fraud operations dashboard for analysts.

It focuses on:

* Sub-500ms hot-path design principles
* Event-driven transaction processing
* Redis-backed idempotency protection
* Redis Lua sliding-window fraud intelligence
* APP scam panic detection
* Receiver-centric mule swarm detection
* PostgreSQL ledger persistence
* Structured JSON observability logs
* Dockerised local execution
* Automated testing, linting, and type checking

---

## 🧭 System Architecture

```mermaid
flowchart LR
    Simulator[Traffic Simulator] -->|transaction events| Kafka[(Kafka)]
    Client[External Client / API Request] -->|POST /api/v1/transactions| API[FastAPI API Gateway]

    API -->|Idempotency-Key check| Redis[(Redis Hot State)]
    API -->|publish transaction| Kafka

    Kafka -->|consume payment_transactions| Consumer[Fraud Engine Consumer]

    Consumer -->|sender velocity + receiver swarm| Redis
    Consumer -->|final decision| Postgres[(PostgreSQL Ledger)]

    Postgres -->|read-only analytics| Dashboard[Fraud Operations Dashboard]

    Consumer -->|structured JSON logs| Logs[Observability Logs]

    Redis -.->|idem:{id}| Idem[API Idempotency]
    Redis -.->|vel:user:{user_id}| SenderVelocity[Sender Velocity]
    Redis -.->|vel:merch:{merchant_id}| ReceiverSwarm[Receiver Swarm]
```

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
* Fraud Operations Dashboard

---

## ✅ Verify the System

### API Health Check

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

### Open the Fraud Dashboard

Once the system is running, open:

```bash
http://localhost:8080
```

The dashboard updates automatically as the traffic simulator generates transactions.

---

## 🖥️ Live Fraud Operations Dashboard

The project includes a browser-based fraud operations dashboard that reads from the PostgreSQL ledger.

The dashboard displays:

* Total processed transaction volume
* Approved, step-up review, and declined counts
* Average fraud risk score
* Latest transaction decisions
* Color-coded decision statuses
* Color-coded fraud reason badges
* Top triggered fraud vectors

Status colors:

| Status | Meaning                 |
| ------ | ----------------------- |
| Green  | Approved transaction    |
| Yellow | Step-up review required |
| Red    | Declined transaction    |

Fraud reason badges:

| Badge   | Meaning                                 |
| ------- | --------------------------------------- |
| Purple  | Receiver swarm / mule-network behaviour |
| Orange  | Sender velocity / bot-like behaviour    |
| Crimson | APP scam panic behaviour                |
| Blue    | New payee risk signal                   |

This makes the system easier to demo than raw console logs and shows how analysts could monitor fraud patterns in real time.

---

## 📡 Example Fraud Engine Log

To view the fraud engine logs:

```bash
docker compose logs -f fraud_engine
```

Example structured decision log:

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

# Architecture & Engineering RFC

## 1. System Overview

This project simulates a production-grade fintech payment authorization system inspired by modern card payment processors and challenger banks.

The architecture prioritises:

* Strict latency awareness for the authorization path
* Bounded effectively-once processing semantics
* Risk-tiered fraud decisions
* Clear separation between API ingestion, fraud decisioning, persistence, and dashboard analytics
* High observability for audit and debugging workflows

---

## 1.1 Fraud Decision Philosophy

During uncertainty or degraded system state, the engine prioritises preventing financial loss over minimising false positives.

The system accepts temporary customer friction when fraud risk is high or infrastructure signals are degraded.

---

## 1.2 Consistency Guarantees by Subsystem

### PostgreSQL Ledger

PostgreSQL acts as the durable source of truth for evaluated transactions.

It stores the final fraud decision, risk score, trace ID, fraud reasons, Redis metrics, and transaction metadata.

### Kafka Event Stream

Kafka acts as the asynchronous transaction transport layer.

The API Gateway and traffic simulator publish transaction events to Kafka. The fraud engine consumes from Kafka and commits offsets only after successful processing.

### Redis Hot State

Redis stores short-lived fraud and idempotency state.

Redis Lua scripts are used for atomic state transitions and sliding-window fraud checks.

### Dashboard

The dashboard is read-only and does not participate in transaction authorization.

It queries PostgreSQL to display operational metrics and the latest fraud decisions.

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

These are intentionally out of scope for the current portfolio version.

---

# 2. Service Ownership

## `api_gateway` — FastAPI Hot Path

The API Gateway owns:

* HTTP request validation
* `Idempotency-Key` enforcement
* Redis idempotency state transitions
* Kafka event publishing
* Health check endpoint

It does not own long-term analytics or ledger persistence.

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
* Structured JSON decision logging
* PostgreSQL ledger writes
* Kafka offset commits after successful processing

It does not own HTTP request handling.

---

## `traffic_simulator` — Calibrated Transaction Generator

The traffic simulator owns synthetic transaction generation.

It produces multiple traffic scenarios:

* Normal customer spending
* APP scam panic transfers
* Mule swarm behaviour
* Bot-like sender velocity behaviour

The simulator is calibrated so normal traffic does not accidentally trigger every fraud rule. Fraud patterns are intentionally injected to make the dashboard meaningful.

---

## `fraud_dashboard` — Read-Only Operations View

The dashboard owns:

* KPI display
* Latest transaction feed
* Top triggered fraud vectors
* Color-coded status badges
* Color-coded fraud reason badges

It reads from PostgreSQL and does not affect the transaction processing path.

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

This models bot-like behaviour, repeated payment attempts, or compromised accounts.

### Receiver Swarm Detection

Redis key:

```text
vel:merch:{merchant_id}
```

Purpose:

Detects whether many unique users are suddenly sending funds to the same merchant or receiver.

This models mule-network and micro-structuring behaviour.

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
* risk_reasons
* redis_eval_ms
* sender_velocity_count
* receiver_unique_sender_count
* received_at

`transaction_id` is the primary key.

Duplicate inserts are ignored using PostgreSQL conflict handling, protecting the ledger from duplicate Kafka delivery or consumer retry behaviour.

---

## 5.2 Redis Hot State

Redis stores ephemeral fraud and idempotency state.

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

Future versions may split approved and declined events into separate downstream topics.

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
redis_unavailable_static_only
```

Kafka offset commits are logged after successful processing.

The dashboard provides a visual layer over the same decision data stored in PostgreSQL.

---

# 7. Testing and Code Quality

The project includes automated testing, linting, and type checking.

## Run Tests

```bash
python -m pytest
```

Current status:

```text
18 passed
```

## Run Ruff

```bash
python -m ruff check .
```

## Run mypy

```bash
python -m mypy .
```

## Test Coverage Includes

* Static risk scoring
* Risk routing boundaries
* Redis sender velocity detection
* Redis receiver swarm detection
* Duplicate transaction handling
* API idempotency behaviour
* Missing idempotency header rejection
* Conflicting payload rejection

## CI/CD

GitHub Actions runs:

* Ruff linting
* mypy type checking
* pytest test suite

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

## View dashboard logs only

```bash
docker compose logs -f fraud_dashboard
```

## Run tests locally

```bash
python -m pytest
```

---

# 9. Completed Features

* FastAPI API Gateway
* Kafka transaction pipeline
* PostgreSQL immutable ledger
* Redis fraud intelligence
* Redis Lua sliding-window checks
* APP scam scoring
* Sender velocity detection
* Receiver swarm detection
* API idempotency
* Trace ID propagation
* Structured JSON logs
* Live fraud operations dashboard
* Color-coded fraud reason badges
* Calibrated traffic simulator scenarios
* Dockerised infrastructure and Python services
* GitHub Actions CI
* Ruff linting
* mypy type checking
* Pytest suite with 18 passing tests
* Mermaid architecture diagram

---

# 10. Roadmap

Planned future improvements:

* Dashboard time-window selector
* Prometheus metrics endpoint
* Grafana dashboard
* More integration tests
* Dead-letter queue for malformed Kafka messages
* Improved FastAPI lifespan handling
* CI status badge in README
* Demo screenshot or GIF
* Optional transaction detail page by `trace_id`

---

# 11. AI Usage Disclosure

AI tools were used during this project as engineering assistants for architecture review, debugging guidance, documentation structure, and production-readiness critique.

All implementation decisions, local debugging, testing, commits, and system validation were owned by me.

The project was not treated as a copy-paste exercise. AI was used similarly to a senior engineering mentor: to challenge assumptions, identify hidden failure modes, and accelerate learning.

---

# 12. Key Learning Outcomes

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
* Dashboard-driven operational visibility
* CI/CD workflows
* Ruff linting
* mypy type checking
* Distributed systems tradeoffs
