&#x20;ADR 0002 — Use Kafka for Event Streaming



&#x20;Status



Accepted



&#x20;Context



The system needs to process transaction events asynchronously after ingestion.



The fraud engine uses Kafka to decouple transaction producers from downstream consumers. This allows the API Gateway and traffic simulator to publish transaction events without directly depending on the consumer implementation.



Kafka also provides a realistic event-driven architecture for a fintech-style backend system.



&#x20;Decision



Use Kafka as the event streaming layer.



The current topic is:





payment\_transactions





The API Gateway and traffic simulator publish transaction events into Kafka.



The fraud engine consumer reads from Kafka, evaluates fraud risk, writes the final decision to PostgreSQL, and commits offsets after successful processing.



&#x20;Consequences



&#x20;Benefits



\* Decouples producers from consumers

\* Supports asynchronous transaction processing

\* Enables replay-safe consumer design

\* Provides realistic event-driven infrastructure

\* Makes it easier to add future consumers for analytics, alerts, or audit processing



&#x20;Tradeoffs



\* Kafka adds operational complexity

\* Docker Compose setup becomes heavier

\* Consumers must handle duplicate or replayed messages safely

\* Kafka should not be treated as the durable financial ledger



&#x20;Important Design Boundary



Kafka is used for asynchronous event transport.



It is not treated as the only mechanism for live authorization decisions because payment authorization systems require deterministic low-latency responses.



The durable source of truth is PostgreSQL, not Kafka.



&#x20;Alternatives Considered



&#x20;Direct API-to-Database Writes



Rejected because it would tightly couple request handling, fraud processing, and persistence.



&#x20;In-Memory Queue



Rejected because it would not survive process restarts and would not model production event streaming.



&#x20;RabbitMQ



Considered, but Kafka better matches the event-streaming and replay-oriented architecture being demonstrated.



