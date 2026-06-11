&#x20;ADR 0001 — Use Redis for Hot Fraud State



&#x20;Status



Accepted



&#x20;Context



The fraud engine needs to evaluate short-lived risk signals quickly.



These signals include:



\* API idempotency state

\* sender velocity windows

\* receiver swarm windows



These checks must remain correct even if the system is horizontally scaled across multiple service instances.



Local Python memory would not be safe because each process would only see the traffic routed to that specific instance. This would make velocity and swarm detection inconsistent under load balancing.



&#x20;Decision



Use Redis as the shared hot-state layer.



Redis stores:



\* idempotency keys

\* sender velocity sliding windows

\* receiver swarm sliding windows



Redis Lua scripts are used for atomic state transitions and sliding-window updates where correctness depends on multiple operations happening together.



&#x20;Consequences



&#x20;Benefits



\* Low-latency fraud checks

\* Shared state across service instances

\* Built-in TTL support for expiring old fraud state

\* Atomic updates through Lua scripts

\* Better scalability than local in-memory state



&#x20;Tradeoffs



\* Redis becomes a critical dependency for hot-path fraud checks

\* Redis outages require fallback or degraded behaviour

\* Lua scripts add implementation complexity

\* Redis state is transient and must not be treated as the durable source of truth



&#x20;Alternatives Considered



&#x20;Local Python Memory



Rejected because it breaks under horizontal scaling.



&#x20;PostgreSQL



Rejected for hot fraud state because frequent sliding-window updates would create unnecessary database load and latency.



&#x20;Kafka



Rejected for hot-path state because Kafka is asynchronous and not suitable for immediate fraud state lookups.



