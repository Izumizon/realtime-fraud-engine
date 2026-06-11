&#x20;ADR 0003 — Use PostgreSQL for the Transaction Ledger



&#x20;Status



Accepted



&#x20;Context



The system needs a durable source of truth for evaluated transaction decisions.



Redis is useful for short-lived hot state, and Kafka is useful for event transport, but neither should be treated as the permanent transaction ledger.



A fraud detection system needs durable records for:



\* auditability

\* debugging

\* dashboard analytics

\* duplicate protection

\* replay-safe consumer behaviour

\* future reconciliation workflows



&#x20;Decision



Use PostgreSQL as the transaction ledger.



Each evaluated transaction is stored in the `transactions` table.



The ledger stores:



\* transaction ID

\* trace ID

\* user ID

\* merchant ID

\* amount

\* currency

\* final status

\* risk score

\* fraud reasons

\* Redis evaluation latency

\* sender velocity count

\* receiver unique sender count

\* received timestamp



transaction\_id is used as the primary key to prevent duplicate ledger records.



&#x20;Consequences



&#x20;Benefits



\* Durable transaction history

\* Strong consistency through ACID transactions

\* Good fit for structured financial records

\* Supports dashboard analytics through SQL queries

\* Protects against duplicate records using primary-key constraints



&#x20;Tradeoffs



\* PostgreSQL should not be overloaded with hot-path sliding-window fraud state

\* Schema changes require care as the project grows

\* Long-term analytics may eventually require separate reporting tables or a data warehouse



&#x20;Alternatives Considered



&#x20;Redis



Rejected as the durable ledger because Redis is transient hot state.



&#x20;Kafka



Rejected as the ledger because Kafka is an event log, not the authoritative queryable transaction store.



&#x20;SQLite



Rejected because PostgreSQL better represents production-style backend infrastructure and async service integration.



