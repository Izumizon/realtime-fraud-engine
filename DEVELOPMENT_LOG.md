\# Development Log — Real-Time Fraud Detection Engine



\## Project Overview



This project is a production-inspired real-time fraud detection and payment authorization engine built with FastAPI, Kafka, Redis, PostgreSQL, and Docker Compose.



The goal was to build a portfolio-grade backend system that demonstrates event-driven architecture, distributed systems thinking, fraud detection logic, API idempotency, observability, testing, and CI/CD practices.



\---



\## Timeline



\### Phase 1 — Architecture and System Design



The project began as an architecture-first exercise. I designed the system around a fintech-style payment authorization flow with clear separation between the hot path and cold path.



The initial design focused on:



\- FastAPI as the API Gateway

\- Kafka as the event stream

\- Redis as the hot-state layer

\- PostgreSQL as the immutable ledger

\- A traffic simulator for synthetic fraud and normal activity



The design also included key distributed systems concepts:



\- bounded effectively-once processing

\- risk-tiered degradation

\- idempotency keys

\- replay-safe consumers

\- structured observability

\- fraud decision explainability boundaries



The architecture was iterated heavily before implementation to clarify service ownership, failure modes, and what was intentionally out of scope.



\---



\### Phase 2 — Core Infrastructure



I created a Docker Compose environment containing:



\- PostgreSQL

\- Redis

\- Kafka

\- Zookeeper



This established the core infrastructure needed for an event-driven fraud detection system.



PostgreSQL was used as the durable ledger, Redis as the low-latency state store, and Kafka as the event broker between the API and the fraud engine.



\---



\### Phase 3 — API Gateway and Kafka Pipeline



I built the FastAPI API Gateway to receive transaction payloads and publish them to Kafka.



The API validates incoming payloads using Pydantic models and adds server-side metadata such as timestamps and trace IDs.



This phase proved that the frontend-facing API could ingest payment events and place them onto the event stream for asynchronous processing.



\---



\### Phase 4 — PostgreSQL Ledger and Kafka Consumer



I implemented the fraud engine Kafka consumer using `aiokafka`.



The consumer reads transaction events from Kafka, calculates an initial fraud risk score, and writes the final decision into PostgreSQL using SQLAlchemy async sessions.



The ledger uses `transaction\_id` as the primary key so duplicate processing cannot corrupt stored transaction records.



This phase created the first end-to-end flow:



FastAPI → Kafka → Consumer → PostgreSQL



\---



\### Phase 5 — Redis Fraud Intelligence Layer



I added a Redis-based intelligence layer using `redis.asyncio` and Lua scripts.



The Redis evaluator implements two real-time fraud checks:



1\. Sender velocity detection  

&#x20;  Tracks how many transaction attempts a user makes inside a 10-minute sliding window.



2\. Receiver swarm detection  

&#x20;  Tracks how many unique users send money to the same merchant inside a 10-minute window.



This expanded the fraud model beyond basic user-level rules and introduced receiver-centric detection for mule-network and micro-structuring behaviour.



\---



\### Phase 6 — API Idempotency



I implemented Redis-backed API idempotency using `Idempotency-Key`.



This protects against:



\- user double-clicks

\- merchant retries

\- network timeouts

\- duplicate client submissions



The idempotency layer supports:



\- first request reservation

\- cached completed responses

\- in-flight duplicate detection

\- conflicting payload rejection



This made the API Gateway much closer to a real payment authorization system.



\---



\### Phase 7 — Trace IDs and Structured Logging



I added end-to-end `trace\_id` propagation through:



FastAPI → Kafka → Consumer → Redis → PostgreSQL



I then replaced human-only print statements in the fraud engine with structured JSON logs.



Each transaction decision log includes:



\- trace ID

\- transaction ID

\- user ID

\- merchant ID

\- amount

\- currency

\- decision status

\- risk score

\- fraud reasons

\- Redis evaluation latency

\- Kafka offset commits



This made the system ready for future observability tooling such as Grafana, Loki, or ELK.



\---



\### Phase 8 — Automated Testing



I added a pytest suite covering:



\- static fraud scoring

\- risk routing boundaries

\- Redis sender velocity detection

\- Redis receiver swarm detection

\- duplicate transaction handling

\- API idempotency behaviour

\- conflicting idempotency payload rejection

\- missing idempotency header rejection



The project reached 18 passing tests.



This phase helped prove that important correctness properties were tested instead of only manually verified.



\---



\### Phase 9 — Full Dockerisation



I expanded Docker Compose so the full system can run with one command.



The Docker setup now starts:



\- PostgreSQL

\- Redis

\- Kafka

\- Zookeeper

\- FastAPI API Gateway

\- Fraud Engine Consumer

\- Traffic Simulator



This changed the project from a manually run collection of scripts into a reproducible local system.



\---



\### Phase 10 — README and Documentation



I updated the README to reflect the real current system.



The README now includes:



\- one-command Docker startup

\- current feature list

\- architecture explanation

\- fraud scoring model

\- idempotency model

\- testing instructions

\- roadmap

\- AI usage disclosure



I also renamed the README correctly from `README.md.md` to `README.md`.



\---



\### Phase 11 — CI/CD



I added GitHub Actions to automatically run the test suite on every push and pull request.



The workflow starts a Redis service for Redis integration tests, installs dependencies, and runs the test suite.



This gave the project a baseline CI pipeline.



\---



\### Phase 12 — Ruff Linting and mypy Type Checking



I added Ruff for linting and mypy for type checking.



This required fixing several real engineering issues:



\- FastAPI `Header(...)` defaults were changed to `Annotated`

\- exception chaining was made explicit with `raise ... from exc`

\- Redis Lua script arguments were normalised for type checking

\- Redis script loading/evaluation was wrapped to handle redis-py typing

\- the traffic simulator was cleaned up to avoid mixed-dictionary type issues



The CI pipeline now checks:



\- Ruff linting

\- mypy type checking

\- pytest test suite



\---



\## Major Technical Problems Solved



\### Docker Startup Race Conditions



PostgreSQL could be marked as started before it was ready to accept connections. This was handled with startup ordering and database initialization logic.



\### Redis Connection Issues on Windows



Redis tests initially failed because `localhost` resolved inconsistently on Windows. Switching Redis test URLs to `127.0.0.1` made the tests deterministic.



\### Kafka Offset Safety



Kafka offsets are committed only after transaction processing succeeds, reducing the risk of losing messages before they are persisted.



\### API Idempotency Edge Cases



The system handles completed duplicates, in-flight duplicates, and same-key different-payload conflicts.



\### Type Checking Async Redis Code



Redis async type hints were broader than runtime behaviour. Helper functions were introduced to normalise Redis script loading and evaluation for mypy.



\---



\## AI Usage



AI tools were used during this project as engineering assistants for architecture review, debugging, documentation structure, and production-readiness critique.



The work was not treated as a copy-paste exercise. I used AI similarly to a senior engineering mentor: to challenge my assumptions, explain tradeoffs, and help identify hidden failure modes.



All implementation decisions, local debugging, test execution, commits, and validation were performed by me.



\---



\## Current Status



The system currently includes:



\- Dockerised full-stack local environment

\- FastAPI API Gateway

\- Kafka event pipeline

\- Redis fraud intelligence layer

\- PostgreSQL transaction ledger

\- traffic simulator

\- API idempotency

\- trace ID propagation

\- structured JSON logs

\- 18 passing tests

\- Ruff linting

\- mypy type checking

\- GitHub Actions CI



\---



\## Next Steps



Planned future improvements:



\- Prometheus metrics endpoint

\- Grafana dashboard

\- architecture diagrams

\- richer traffic simulator scenarios

\- more integration tests

\- dead-letter queue for malformed Kafka messages

\- improved FastAPI lifespan handling

\- CI badge in README

