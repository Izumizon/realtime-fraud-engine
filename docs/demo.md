# Demo Guide — Real-Time Fraud Detection Engine



This guide explains how to run and demonstrate the project locally.







#### 1\. Start the System



From the project root:



\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

*docker compose up --build*



This starts:



PostgreSQL ledger

Redis hot-state cache

Kafka event broker

Zookeeper

FastAPI API Gateway

Fraud Engine Consumer

Traffic Simulator

Fraud Operations Dashboard



Wait until the services are running and the traffic simulator begins producing events.





\----------------------------------------------------------------









#### 2\. Check API Health





In a second terminal:



curl http://localhost:8000/health



It should print:



**{**

&#x20; **"status": "healthy"**

**}**

\----------------------------------------------------------------









#### 3\. Open the Dashboard



Open:



http://localhost:8080



The dashboard shows:



Window volume

All-time volume

Approved, review, and declined counts

Decision split visualisation

Risk breakdown visualisation

Latest transaction decisions

Top triggered fraud vectors

Color-coded fraud reason badges

\-----------------------------------------------------------------







#### 4\. Change Time Windows



Use the dashboard buttons:

\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

5m | 1h | 24h | All

\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_





These change the analytics window for the KPI cards, decision split, risk breakdown, top fraud vectors, and latest transaction feed.

\----------------------------------------------------------------





#### 5\. Open a Transaction Detail Page



Click any Trace ID in the dashboard feed.



This opens:

http://localhost:8080/transactions/<trace\_id>



The detail page shows:



transaction ID

trace ID

user ID

merchant ID

amount

currency

status

risk score

fraud reasons

Redis evaluation time

sender velocity count

receiver unique sender count

received timestamp



\----------------------------------------------------------------













6\. View Fraud Engine Logs



To follow the fraud engine decision logs:

\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

docker compose logs -f fraud\_engine

\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_



Each decision log includes the risk score, status, reasons, Redis metrics, and Kafka offset commit behaviour.

\----------------------------------------------------------------













7\. What to Point Out During a Demo



Important talking points:



Redis is used for hot fraud state and idempotency.

Kafka decouples transaction producers from the fraud engine consumer.

PostgreSQL stores the durable transaction ledger.

The dashboard is read-only and queries the ledger.

Fraud decisions are explainable through risk reasons.

Transaction detail pages support analyst investigation.

The test suite covers scoring, Redis fraud signals, API idempotency, and dashboard APIs.



\----------------------------------------------------------------

8\. Run Quality Checks





\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_



python -m ruff check .

python -m mypy .

docker compose up -d redis

python -m pytest

\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_







Expected result:





\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_



24 passed

\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_





\----------------------------------------------------------------

9\. Reset the Demo Data



To reset all data and start fresh:





\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_



docker compose down -v

docker compose up --build

\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_









This removes Docker volumes and recreates PostgreSQL/Redis state.







\-----------------------------------------------------------------















