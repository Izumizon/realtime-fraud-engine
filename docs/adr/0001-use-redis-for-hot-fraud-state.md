\# ADR 0001 — Use Redis for Hot Fraud State



\## Status



Accepted



\## Context



The fraud engine needs to evaluate short-lived risk signals quickly, including sender velocity, receiver swarm behaviour, and API idempotency state.



These checks must work even if the application is horizontally scaled across multiple service instances. Local Python memory would not be safe because each instance would only see its own traffic.



\## Decision



Use Redis as the shared hot-state layer.



Redis stores:



\- API idempotency keys

\- sender velocity sliding windows

\- receiver swarm sliding windows



Redis Lua scripts are used where atomic updates are required.



\## Consequences



\### Benefits



\- Low-latency fraud checks

\- Shared state across service instances

\- TTL support for automatically expiring old fraud state

\- Atomic updates through Lua scripts

\- Better scalability than local in-memory state



\### Tradeoffs



\- Redis becomes a dependency for the hot path

\- Redis outages require fallback behaviour

\- Lua scripts add implementation complexity

\- Redis state is transient and must not be treated as the durable source of truth



\## Alternatives Considered



\### Local Python Memory



Rejected because it would break under horizontal scaling.



\### PostgreSQL



Rejected for hot fraud state because repeated sliding-window updates would add unnecessary latency and database load.



\### Kafka



Rejected for hot-path state because Kafka is asynchronous and not suitable for immediate authorization state lookups.

