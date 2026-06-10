# Development Log — Real-Time Fraud Detection Engine

## Project Overview

This project is a production-inspired real-time fraud detection and payment authorization engine built with **FastAPI**, **Kafka**, **Redis**, **PostgreSQL**, and **Docker Compose**.

The goal was to build a portfolio-grade backend system that demonstrates:

* event-driven architecture
* distributed systems thinking
* real-time fraud detection
* Redis-backed hot-state processing
* API idempotency
* PostgreSQL ledger persistence
* structured observability
* automated testing
* CI/CD discipline
* a live operational dashboard

The project was designed with fintech-style constraints in mind: low-latency authorization, duplicate request protection, replay-safe processing, explainable fraud decisions, and clear separation between transaction ingestion, fraud decisioning, persistence, and analytics.

---

## Why This Project Was Built

The project began as a targeted portfolio project for backend/Python fintech engineering roles.

Instead of building a generic CRUD application, I wanted to build something closer to the kind of system used in financial infrastructure:

* receiving payment-like events
* evaluating fraud risk
* handling duplicate requests safely
* maintaining a durable ledger
* processing real-time streams
* exposing operational visibility
* testing correctness with automation

The project evolved from a simple fraud scoring API into a distributed local system with multiple services running together through Docker Compose.

---

# Timeline

## Phase 1 — Architecture and System Design

The project began with architecture planning before implementation.

The initial design focused on a fintech-style transaction pipeline using:

* FastAPI as the API Gateway
* Kafka as the event stream
* Redis as the hot-state layer
* PostgreSQL as the durable ledger
* a traffic simulator to generate normal and suspicious transaction activity

The architecture was shaped around several distributed systems concerns:

* hot path vs cold path separation
* bounded effectively-once processing
* API idempotency
* replay-safe consumers
* fraud decision explainability
* risk-tiered degradation
* observability and auditability

A major early design decision was recognizing that Kafka should not be the only mechanism for live authorization decisions. Kafka is useful for asynchronous event processing, analytics, and auditing, but real-time payment authorization requires low-latency synchronous logic in the hot path.

---

## Phase 2 — Core Infrastructure

I created the initial Docker Compose environment with:

* PostgreSQL
* Redis
* Kafka
* Zookeeper

This established the infrastructure foundation for the system.

PostgreSQL became the durable transaction ledger. Redis became the low-latency shared state layer. Kafka became the event broker for transaction events.

This phase also involved practical Docker debugging, including container startup ordering, service names, ports, health checks, and environment configuration.

---

## Phase 3 — API Gateway and Kafka Pipeline

I built the FastAPI API Gateway to receive transaction requests and publish events to Kafka.

The API Gateway validates incoming payloads using Pydantic models and adds server-generated metadata such as timestamps and trace IDs.

At this stage, the system could accept transaction-like requests and place them onto the Kafka event stream for asynchronous processing.

This created the first API-facing entry point into the system.

---

## Phase 4 — PostgreSQL Ledger and Kafka Consumer

I implemented the fraud engine Kafka consumer using `aiokafka`.

The consumer reads transaction events from Kafka, evaluates fraud risk, and writes the resulting decision into PostgreSQL using SQLAlchemy async sessions.

This phase created the first full end-to-end pipeline:

```text
FastAPI → Kafka → Fraud Engine Consumer → PostgreSQL
```

The PostgreSQL table used `transaction_id` as the primary key so duplicate Kafka deliveries could not create duplicate ledger records.

This introduced the idea of the ledger as the durable source of truth for evaluated transactions.

---

## Phase 5 — Behavioural Fraud Scoring

I added deterministic fraud scoring based on behavioural metadata.

The first static fraud signals were:

* new payee
* password reset in the last 24 hours
* unusually fast transaction completion time

The scoring model uses a 0–100 risk score and maps scores into three decision bands:

| Risk Score | Decision       |
| ---------- | -------------- |
| 0–39       | APPROVED       |
| 40–69      | STEP-UP_REVIEW |
| 70–100     | DECLINED       |

This made the fraud engine explainable. Instead of returning only true or false, each decision includes a score and a list of reasons.

---

## Phase 6 — Redis Fraud Intelligence Layer

I added a Redis-backed fraud intelligence layer using `redis.asyncio` and Lua scripts.

The Redis evaluator implements two real-time fraud checks:

### Sender Velocity Detection

Tracks how many unique transaction attempts a user makes inside a sliding time window.

This models behaviour such as card testing, bot-like retries, or compromised account activity.

### Receiver Swarm Detection

Tracks how many unique users send money to the same merchant inside a sliding time window.

This models mule-network behaviour and receiver-centric fraud patterns.

Redis was chosen because local Python memory would not work correctly once the API or consumer is horizontally scaled. Shared fraud state must live outside a single process.

The Redis logic uses atomic Lua scripts so the sliding-window updates and counts happen safely under concurrent access.

---

## Phase 7 — API Idempotency

I implemented Redis-backed API idempotency using the `Idempotency-Key` header.

This protects the API Gateway against:

* user double-clicks
* merchant retries
* network timeouts
* repeated identical client submissions
* same-key different-payload misuse

The idempotency design supports:

| Scenario                                      | Behaviour                                 |
| --------------------------------------------- | ----------------------------------------- |
| First request with a new key                  | Reserve key and publish to Kafka          |
| Same key with same payload after completion   | Return cached response                    |
| Same key while original request is processing | Return 202 Accepted                       |
| Same key with different payload               | Return 409 Conflict                       |
| Internal failure                              | Mark state as failed so retry is possible |

This phase made the API much closer to a real payment-style service, where duplicate processing can cause serious correctness issues.

---

## Phase 8 — Trace IDs and Structured Logging

I added end-to-end `trace_id` propagation through the transaction pipeline:

```text
API Gateway → Kafka → Fraud Engine → Redis → PostgreSQL
```

I then replaced human-only print output with structured JSON decision logs.

Each fraud decision log includes:

* event name
* service name
* trace ID
* transaction ID
* user ID
* merchant ID
* amount
* currency
* decision status
* risk score
* fraud reasons
* Redis evaluation latency
* sender velocity count
* receiver unique sender count
* Kafka offset commits

This made the system easier to debug and prepared it for future observability tooling such as Grafana, Loki, ELK, or Prometheus.

---

## Phase 9 — Automated Testing

I added a pytest suite covering the most important correctness behaviour.

The tests cover:

* static fraud scoring
* risk routing boundaries
* Redis sender velocity detection
* Redis receiver swarm detection
* duplicate transaction handling
* API idempotency behaviour
* missing idempotency header rejection
* conflicting idempotency payload rejection

The project reached:

```text
18 passing tests
```

This phase was important because fraud and payment-style systems should not rely only on manual testing. The tests prove key rules and edge cases are repeatable.

---

## Phase 10 — Full Dockerisation

I expanded Docker Compose so the full system can run with one command.

The Docker setup starts:

* PostgreSQL
* Redis
* Kafka
* Zookeeper
* FastAPI API Gateway
* Fraud Engine Consumer
* Traffic Simulator
* Fraud Operations Dashboard

This changed the project from a manually run collection of scripts into a reproducible local system.

The full system can now be started with:

```bash
docker compose up --build
```

---

## Phase 11 — Calibrated Traffic Simulator

I improved the traffic simulator so the dashboard would show meaningful patterns instead of random noise.

The simulator now generates:

* normal customer spending
* APP scam panic transfers
* mule swarm behaviour
* bot-like sender velocity behaviour

A key issue was that the original simulator had too few merchants and users, which caused normal traffic to eventually trigger receiver swarm detection everywhere.

The simulator was recalibrated with more normal users and merchants, plus specific intentional attack scenarios. This made the dashboard more realistic and easier to demo.

---

## Phase 12 — README and Documentation

I updated the README to reflect the real system.

The README now includes:

* project overview
* architecture diagram
* one-command Docker startup
* dashboard instructions
* fraud scoring model
* idempotency model
* data architecture
* testing instructions
* roadmap
* AI usage disclosure
* demo screenshot

I also fixed the repository documentation naming issue by replacing the duplicate `README.md.md` with the correct `README.md`.

---

## Phase 13 — GitHub Actions CI

I added GitHub Actions to automatically run checks on every push.

The CI pipeline runs:

* Ruff linting
* mypy type checking
* pytest test suite

The workflow also starts Redis for the Redis integration tests.

This gave the project a real quality gate and made the repository more professional.

---

## Phase 14 — Ruff Linting and mypy Type Checking

I added Ruff for linting and mypy for static type checking.

This phase caught and forced me to fix several real engineering issues:

* FastAPI `Header(...)` defaults were changed to `Annotated`
* exception chaining was made explicit using `raise ... from exc`
* Redis async typing had to be handled carefully
* Redis script loading/evaluation required helper functions for type safety
* imports and formatting were cleaned up
* dictionary typing in the traffic simulator was improved

This phase made the codebase cleaner, safer, and easier to maintain.

---

## Phase 15 — Live Fraud Operations Dashboard

I added a browser-based Fraud Operations Dashboard using FastAPI and vanilla HTML/CSS/JavaScript.

The dashboard reads from the PostgreSQL ledger and displays:

* total transaction volume
* approved transaction count
* step-up review count
* declined transaction count
* average risk score
* latest transaction decisions
* top fraud vectors
* color-coded decision statuses
* color-coded fraud reason badges

This made the system much easier to demo than raw terminal logs.

Instead of only showing backend output, the project now has an operational view similar to what a fraud analyst might use.

---

## Phase 16 — Dashboard Time Filters

I added dashboard time filters:

```text
5m | 1h | 24h | All
```

The dashboard API now supports:

```text
/api/stats?window=5m
/api/stats?window=1h
/api/stats?window=24h
/api/stats?window=all
```

This made the dashboard more realistic because fraud operations teams care about what is happening right now, not only all-time totals.

I also added:

* active window labels
* window volume
* all-time volume comparison
* empty states for time windows with no transactions

This made the filter behaviour clear during demos.

---

## Phase 17 — Dashboard Visual Analytics Upgrade

I improved the dashboard visual design by adding:

* Decision Split bar
* Risk Breakdown bar
* percentage breakdowns for approved, review, and declined transactions
* low, medium, and high risk breakdowns
* cleaner visual hierarchy

This made the dashboard easier to understand at a glance.

The dashboard now answers two important operational questions quickly:

```text
What is the current decision split?
How risky is the current transaction stream?
```

This phase made the project look more like a real fraud operations tool rather than just a table of events.

---

## Phase 18 — Demo Screenshot

I added a dashboard screenshot to the README.

This gives GitHub visitors an immediate visual understanding of the project without needing to clone and run it first.

The screenshot shows:

* active time window
* KPI cards
* decision split
* risk breakdown
* live transaction feed
* fraud reason badges
* top fraud vectors

This improved the portfolio presentation of the project.

---

# Major Technical Problems Solved

## Docker and Service Startup Issues

I dealt with multiple Docker-related issues, including services starting before dependencies were ready.

PostgreSQL needed health checks and initialization ordering so tables existed before the app services attempted to write data.

Kafka and Zookeeper also required careful Docker Compose configuration so the broker was reachable from both local development and other containers.

---

## Windows Localhost and Redis Testing

Redis tests initially had connection inconsistencies on Windows.

Switching Redis integration test URLs to `127.0.0.1` made the tests more deterministic in the local Windows environment.

---

## Dockerfile Naming and Build Context

The Docker build initially failed because the file was saved as `Dockerfile.txt` instead of `Dockerfile`.

Fixing the file name allowed Docker Compose to build the Python services correctly.

---

## PowerShell curl JSON Quoting

Sending JSON payloads through PowerShell `curl.exe` caused escaping issues.

This helped clarify the difference between PowerShell aliases, real `curl.exe`, JSON quoting, and HTTP request formatting on Windows.

---

## GitHub Workflow Permissions

Pushing the GitHub Actions workflow initially failed because the authentication token did not have the required workflow scope.

Re-authenticating through the browser fixed the push.

---

## Ruff and mypy Failures

Ruff and mypy caught several issues that the app could still run with locally.

These included:

* import ordering
* unused imports
* FastAPI argument default warnings
* exception chaining
* Redis async typing
* dashboard typing issues

Fixing these improved the quality of the codebase.

---

## Dashboard Runtime and Syntax Issues

While building the dashboard, I hit several issues caused by:

* Markdown code fences accidentally pasted into Python files
* incorrect indentation in async database blocks
* Docker containers running older builds
* endpoint responses not matching the current local code
* dashboard services needing forced rebuilds

These were resolved through local syntax checks, Docker logs, forced rebuilds, and endpoint verification.

---

# Architecture Decisions and Tradeoffs

## Kafka as Cold Path Infrastructure

Kafka is used for event transport and asynchronous processing.

The project intentionally treats Kafka as a cold-path or event pipeline component rather than the only mechanism for live authorization decisions.

This distinction matters because real payment authorization requires deterministic low-latency responses, while Kafka is better suited for decoupled downstream processing, analytics, and audit flows.

---

## Redis for Hot State

Redis is used for short-lived, low-latency state such as:

* idempotency keys
* sender velocity windows
* receiver swarm windows

This avoids relying on local Python memory, which would fail under horizontal scaling.

---

## PostgreSQL as Durable Ledger

PostgreSQL is used as the durable transaction ledger.

The ledger stores final decisions and supporting metadata such as fraud reasons and Redis metrics.

Using PostgreSQL gives the system a durable source of truth separate from transient Redis state and Kafka transport.

---

## Explainable Rule-Based Scoring

The fraud model is intentionally explainable.

Instead of using a black-box machine learning model, the current version uses deterministic scoring rules and Redis dynamic fraud signals.

Every decision can include specific reasons, such as:

* `new_payee`
* `recent_password_reset`
* `panic_execution_speed`
* `sender_velocity_exceeded`
* `receiver_swarm_detected`

This is important for auditability and interview explanation.

---

## Dashboard Without a Frontend Framework

The dashboard was built with FastAPI, HTML, CSS, and vanilla JavaScript instead of React or another frontend framework.

This kept the project backend-focused while still making it visually demo-friendly.

The goal was operational clarity, not frontend complexity.

---

# AI Usage and Ownership

AI tools were used during this project as engineering assistants.

They helped with:

* architecture review
* debugging guidance
* production-readiness critique
* documentation structure
* test planning
* explaining distributed systems tradeoffs

The project was not treated as a copy-paste exercise.

I used AI similarly to a senior engineering mentor: to challenge assumptions, suggest tradeoffs, and help identify hidden failure modes.

All implementation decisions, local debugging, testing, commits, validation, and final project direction were owned by me.

---

# What I Learned

This project strengthened my understanding of:

* FastAPI service design
* async Python
* Kafka producers and consumers
* Redis Lua scripting
* sliding-window fraud detection
* PostgreSQL ledger design
* API idempotency
* trace ID propagation
* structured JSON logging
* Docker Compose orchestration
* GitHub Actions CI
* Ruff linting
* mypy type checking
* dashboard-driven operational visibility
* distributed systems tradeoffs
* debugging real integration issues

The biggest learning outcome was that building production-inspired systems is not just about writing feature code. A large part of engineering is handling failure modes, testing edge cases, observing system behaviour, and making tradeoffs explicit.

---

# Current Status

The project currently includes:

* Dockerised full-stack local environment
* FastAPI API Gateway
* Kafka event pipeline
* Redis fraud intelligence layer
* PostgreSQL transaction ledger
* traffic simulator
* API idempotency
* trace ID propagation
* structured JSON logs
* live fraud operations dashboard
* dashboard time filters
* dashboard visual analytics
* README demo screenshot
* 18 passing tests
* Ruff linting
* mypy type checking
* GitHub Actions CI

The project is now portfolio-ready as a backend engineering showcase.

---

# Future Improvements

Planned future improvements:

* dashboard API tests
* transaction detail page by `trace_id`
* FastAPI lifespan migration to replace deprecated `on_event`
* dead-letter queue for malformed Kafka messages
* Prometheus metrics endpoint
* Grafana dashboard
* CI status badge in README
* more integration tests
* optional load testing with Locust
* Architecture Decision Records in `docs/adr`

---

# Final Reflection

This project started as an idea for a fintech portfolio project and became a realistic local fraud detection platform.

The most valuable part was not only building the services, but learning how the pieces interact:

```text
API Gateway → Kafka → Fraud Engine → Redis → PostgreSQL → Dashboard
```

Each phase exposed a different kind of engineering challenge: infrastructure, correctness, state management, observability, testing, typing, and presentation.

The final result is a project that demonstrates both practical Python backend development and deeper distributed systems thinking.
