# 📖 Real-Time Fraud Detection Engine

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Docker](https://img.shields.io/badge/docker-enabled-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green.svg)

Production-inspired fraud detection and payment authorization platform focused on:

- ≤ 500ms P99 authorization latency
- Effectively-once processing semantics
- Risk-tiered degradation
- Distributed systems resiliency
- Observability and auditability

## 🚀 Quick Start

This system follows 12-Factor App principles and runs entirely in Docker.

### Prerequisites

- Docker
- Docker Compose
- Python 3.10+

### Run the System

```bash
# Clone the repository
git clone https://github.com/yourusername/realtime-fraud-engine.git
cd realtime-fraud-engine

# Start infrastructure and services
docker-compose up -d

# Run the traffic simulator
python scripts/traffic_simulator.py
```

### Verify Health

```bash
curl http://localhost:8000/health
```

Expected response:

```json
{
  "status": "healthy"
}
```

---

# Architecture & Engineering RFC

## 1. System Overview & Philosophy

This project simulates a production-grade fintech payment authorization system inspired by modern card payment processors. This architecture minimises known distributed failure modes under defined constraints, prioritising:

- Strict P99 latency target (≤ 500ms hot path)
- Effectively-once processing semantics (bounded guarantees)
- Risk-tiered degradation under partial system failure
- Clear separation between decisioning and analytics
- High observability and auditability for financial compliance

### 1.1 Fraud Decision Philosophy

During conditions of uncertainty or degraded state, the system prioritises preventing financial loss over minimising false positives. We accept a temporary increase in customer friction (declines) to protect the core ledger integrity.

### 1.2 Consistency Guarantees by Subsystem

- PostgreSQL (Ledger): Strong consistency (ACID). The absolute system of record.
- Kafka (Event Stream): Eventual consistency. Acts strictly as a derived log, not a system of record.
- Redis (Hot State): Single-region, single-writer per key partition with atomic operations; ordering guarantees are best-effort per keyspace.
- Merchant Trust Tiers: Eventual consistency (staleness tolerance bounded to ≤ 5 minutes).

### 1.3 System Boundaries (What this system does NOT do)

- No real-time ML model training in the hot path.
- No distributed consensus algorithms (e.g., Raft/Paxos).
- No cross-region active-active database replication.
- No blockchain or exotic consistency models.

### 1.4 Explicit Uncertainty Boundaries (Accepted Risks)

- Prolonged Global Network Partitions.
- Idempotency Race Window.
- Compound Outages (Config + Redis).

## 2. System Architecture & Service Ownership

### api_gateway (FastAPI) — Hot Path / Decision Engine

Owns HTTP request validation, idempotency lifecycle management, synchronous Redis state operations, fraud rule execution, and final APPROVE/DECLINE decisions.

#### Inbound API Contract

```json
{
  "user_id": "a1b2c3d4",
  "merchant_id": "m-9921",
  "amount": 1050,
  "currency": "GBP",
  "device_ip": "192.168.1.1"
}
```

### fraud_engine (Kafka Consumers) — Cold Path / Intelligence Layer

Owns immutable ledger writes, risk profile recalculation, and statistical feature aggregation.

### traffic_simulator — Chaos & Load Generator

Owns normal traffic generation, attack simulation, and infrastructure failure injection.

## 3. Concurrency, State & Time Semantics

### 3.1 Idempotency State Machine (Redis-Based)

- RECEIVED
- PROCESSING
- COMPLETED

State transitions are enforced under normal operating conditions using Redis CAS semantics via Lua scripts.

### 3.2 Time Consistency Model

All timestamps are server-generated, UTC-based, and immutable.

### 3.3 Effectively-Once Processing Semantics

Achieved via HTTP idempotency keys, Redis atomic guards, Kafka event deduplication, and replay-safe consumers.

## 4. Performance, Scaling & System Limits

### 4.1 Hot Path Latency Budget

- API validation: 5–10ms
- Redis idempotency: 1–3ms
- Redis fraud checks: 1–5ms
- Rule evaluation: 1–10ms
- Network overhead: 10–30ms
- Safety buffer: ~300–350ms

### 4.2 System Limits & Capacity

Designed for a hard limit of 10,000 TPS per region.

### 4.3 Scaling Model & Tradeoffs

- API Gateway: Stateless horizontal scaling
- Redis: Redis Cluster
- Kafka: Partitioned by user_id

## 5. Resilience & Risk-Aware Degradation

### 5.1 Failure Matrix

| Component | Failure State | Behaviour |
|------------|------------|------------|
| Redis | Down | Conservative fallback mode |
| Kafka | Lagging | Analytics delayed |
| PostgreSQL | Slow writes | Load shedding / fail safely |
| Config Service | Down | Last-known-good configuration |

### 5.2 Risk-Aware Degradation Details

Dynamic state loss results in stateless in-memory heuristics and cached configuration fallback.

## 6. Security & Threat Model

- Replay attacks prevented via idempotency keys.
- Double-spend attempts mitigated via idempotency locks and uniqueness constraints.
- Velocity attacks prevented via Redis sliding windows.
- Bot traffic prevented via API Gateway rate limiting.

## 7. Data Architecture

### 7.1 PostgreSQL (Immutable Ledger)

- Append-only design.
- Transactional Outbox Pattern.
- Database idempotency using transaction_id primary key.
- Reconciliation jobs.

### 7.2 Redis (Hot State)

- idem:{id}
- vel:{user_id}

### 7.3 Kafka (Event Streams)

- tx.approved
- tx.declined

## 8. Observability, Audit & Explainability

### 8.1 Distributed Tracing & Metrics

trace_id propagation through API → Redis → Kafka → PostgreSQL.

### 8.2 Incident Alerting Model

Leading and lagging indicators tracked through Grafana and alerting systems.

### 8.3 Decision Audit Logs

Immutable audit trail for fraud decisions.

### 8.4 Fraud Explainability (Internal Only)

Internal explainability endpoint. External clients receive only generic decline responses.

## 9. Testing, Infrastructure & CI/CD

### 9.1 Quality Assurance (TDD)

- Unit Testing (pytest)
- Integration Testing (testcontainers)
- Load & Chaos Testing

### 9.2 Configuration & Local Execution

12-Factor App principles with Docker Compose deployment.

### 9.3 CI/CD Pipeline (GitHub Actions)

mypy, ruff, testing, and merge gates.
