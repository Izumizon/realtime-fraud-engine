import json
from uuid import uuid4
from fastapi import FastAPI, HTTPException, status, Header
from pydantic import BaseModel, Field
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer
from config import settings

app = FastAPI(title=settings.PROJECT_NAME, version="1.0.0")
producer = None

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

class BehavioralMetadata(BaseModel):
    time_to_complete_ms: int = Field(ge=0)
    is_new_payee: bool
    password_reset_24h: bool


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    merchant_id: str

    # Integer minor units only.
    # Example: £10.50 = 1050
    amount: int = Field(ge=0)

    currency: str = Field(min_length=3, max_length=3)
    device_ip: str
    behavioral_metadata: BehavioralMetadata


@app.on_event("startup")
async def startup_event():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    print("✅ Kafka Gateway Online.")


@app.on_event("shutdown")
async def shutdown_event():
    global producer
    if producer:
        await producer.stop()
@app.post("/api/v1/transactions", status_code=status.HTTP_201_CREATED)
async def receive_transaction(
    tx: Transaction,
    x_trace_id: str | None = Header(default=None, alias="X-Trace-Id"),
):
    try:
        trace_id = x_trace_id or str(uuid4())

        transaction_data = tx.model_dump()
        transaction_data["trace_id"] = trace_id
        transaction_data["received_at"] = datetime.now(timezone.utc).isoformat()

        await producer.send_and_wait(
            topic=settings.KAFKA_TOPIC_TRANSACTIONS,
            value=transaction_data,
        )

        return {
            "status": "QUEUED",
            "trace_id": trace_id,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))