import asyncio
import json
import logging
from typing import Any
from uuid import UUID, uuid4

from aiokafka import AIOKafkaConsumer
from redis.exceptions import RedisError
from sqlalchemy.dialects.postgresql import insert

from config import settings
from database import AsyncSessionLocal
from models import TransactionRecord
from redis_fraud_evaluator import (
    FraudRuleConfig,
    RedisFraudEvaluator,
    RedisSettings,
    TransactionRiskInput,
    create_redis_client,
)

# ---------------------------------------------------------------------------
# Structured logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)

logger = logging.getLogger("fraud_engine")


def emit_log(event: str, level: str = "info", **fields: Any) -> None:
    """
    Emit one structured JSON log line.

    Why JSON logs?
    - Easy to search by trace_id.
    - Easy to ingest into Grafana Loki / ELK later.
    - Better for audit trails than human-only print strings.
    """

    payload = {
        "event": event,
        "service": "fraud_engine",
        **fields,
    }

    message = json.dumps(
        payload,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )

    log_method = getattr(logger, level, logger.info)
    log_method(message)


# ---------------------------------------------------------------------------
# Risk scoring
# ---------------------------------------------------------------------------


def calculate_static_risk_score(tx_data: dict[str, Any]) -> tuple[int, list[str]]:
    """
    Static 0-100 risk scoring.

    This catches APP scam-style behaviour:
    - new payee
    - recent password reset
    - unusually fast transaction execution

    This function is intentionally pure and deterministic.
    No IO. No Redis. No database.
    """

    score = 0
    reasons: list[str] = []

    behavior = tx_data.get("behavioral_metadata", {})

    if behavior.get("is_new_payee"):
        score += 20
        reasons.append("new_payee")

    if behavior.get("password_reset_24h"):
        score += 30
        reasons.append("recent_password_reset")

    if behavior.get("time_to_complete_ms", 60_000) < 3_000:
        score += 40
        reasons.append("panic_execution_speed")

    return min(score, 100), reasons


def route_transaction(risk_score: int) -> str:
    """
    Convert final risk score into business decision.

    0-39   -> APPROVED
    40-69  -> STEP-UP_REVIEW
    70-100 -> DECLINED
    """

    if risk_score < 40:
        return "APPROVED"

    if risk_score < 70:
        return "STEP-UP_REVIEW"

    return "DECLINED"


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


async def save_transaction_to_ledger(
    tx_data: dict[str, Any],
    final_status: str,
    risk_score: int,
) -> None:
    """
    Persist the evaluated transaction to the immutable PostgreSQL ledger.

    Uses ON CONFLICT DO NOTHING so Kafka redelivery or duplicate processing
    cannot corrupt the ledger or crash the consumer.
    """

    stmt = (
        insert(TransactionRecord)
        .values(
            transaction_id=tx_data["transaction_id"],
            trace_id=tx_data["trace_id"],
            user_id=tx_data["user_id"],
            merchant_id=tx_data["merchant_id"],
            amount=tx_data["amount"],
            currency=tx_data["currency"],
            status=final_status,
            risk_score=risk_score,
        )
        .on_conflict_do_nothing(index_elements=["transaction_id"])
    )

    async with AsyncSessionLocal() as session:
        async with session.begin():
            await session.execute(stmt)


# ---------------------------------------------------------------------------
# Transaction processing
# ---------------------------------------------------------------------------


async def process_transaction(
    tx_data: dict[str, Any],
    redis_evaluator: RedisFraudEvaluator,
) -> None:
    """
    Full transaction evaluation pipeline.

    Flow:
    1. Ensure trace_id exists.
    2. Calculate deterministic APP scam risk.
    3. Evaluate Redis dynamic risk:
       - sender velocity
       - receiver swarm
    4. Route final decision.
    5. Emit structured audit-style decision log.
    6. Persist to PostgreSQL ledger.
    """

    tx_data.setdefault("trace_id", str(uuid4()))

    trace_id = tx_data["trace_id"]
    transaction_id = tx_data["transaction_id"]
    user_id = tx_data["user_id"]
    merchant_id = tx_data["merchant_id"]

    static_score, static_reasons = calculate_static_risk_score(tx_data)

    redis_reasons: list[str] = []
    redis_eval_ms = 0.0
    sender_velocity_count: int | None = None
    receiver_unique_sender_count: int | None = None

    try:
        redis_input = TransactionRiskInput(
            transaction_id=UUID(transaction_id),
            user_id=user_id,
            merchant_id=merchant_id,
            amount=tx_data["amount"],
            currency=tx_data["currency"],
        )

        redis_result = await redis_evaluator.evaluate_transaction(
            transaction=redis_input,
            base_risk_score=static_score,
        )

        final_score = redis_result.risk_score
        redis_reasons = redis_result.reasons
        redis_eval_ms = redis_result.redis_eval_time_ms
        sender_velocity_count = redis_result.sender_velocity_count
        receiver_unique_sender_count = redis_result.receiver_unique_sender_count

    except RedisError as exc:
        # Redis failure should not crash the consumer.
        # In the cold-path consumer, we degrade to static-only scoring.
        final_score = static_score
        redis_reasons = ["redis_unavailable_static_only"]

        emit_log(
            event="redis_evaluation_unavailable",
            level="warning",
            trace_id=trace_id,
            transaction_id=transaction_id,
            user_id=user_id,
            merchant_id=merchant_id,
            error=str(exc),
        )

    except Exception as exc:
        # Malformed payloads should be visible and should not silently poison
        # the stream. In a larger system this would go to a DLQ.
        emit_log(
            event="transaction_evaluation_failed",
            level="error",
            trace_id=trace_id,
            transaction_id=transaction_id,
            user_id=user_id,
            merchant_id=merchant_id,
            error=str(exc),
        )

        raise RuntimeError(f"Failed to evaluate transaction payload: {exc}") from exc

    final_status = route_transaction(final_score)
    all_reasons = static_reasons + redis_reasons

    emit_log(
        event="transaction_decision",
        trace_id=trace_id,
        transaction_id=transaction_id,
        user_id=user_id,
        merchant_id=merchant_id,
        amount=tx_data["amount"],
        currency=tx_data["currency"],
        status=final_status,
        risk_score=final_score,
        reasons=all_reasons,
        redis_eval_ms=round(redis_eval_ms, 3),
        sender_velocity_count=sender_velocity_count,
        receiver_unique_sender_count=receiver_unique_sender_count,
    )

    await save_transaction_to_ledger(
        tx_data=tx_data,
        final_status=final_status,
        risk_score=final_score,
    )


# ---------------------------------------------------------------------------
# Kafka consumer loop
# ---------------------------------------------------------------------------


async def consume_events() -> None:
    """
    Main Kafka consumer loop.

    Startup order:
    1. Start Redis evaluator first.
    2. Only create/start Kafka consumer after Redis is ready.
    3. Commit Kafka offset only after PostgreSQL persistence succeeds.
    """

    emit_log(
        event="fraud_engine_starting",
        kafka_bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        redis_url=settings.REDIS_URL,
    )

    redis_client = create_redis_client(
        RedisSettings(
            url=settings.REDIS_URL,
            max_connections=50,
            socket_timeout_seconds=1.0,
            socket_connect_timeout_seconds=2.0,
        )
    )

    redis_evaluator = RedisFraudEvaluator(
        redis_client=redis_client,
        config=FraudRuleConfig(
            window_seconds=600,
            # Demo mode
            sender_velocity_threshold=5,
            receiver_swarm_threshold=10,
            sender_velocity_penalty=35,
            receiver_swarm_penalty=45,
        ),
    )

    consumer = None

    try:
        # Start Redis first so Kafka is not left unclosed if Redis fails.
        await redis_evaluator.start()

        emit_log(
            event="redis_intelligence_layer_online",
            redis_url=settings.REDIS_URL,
        )

        consumer = AIOKafkaConsumer(
            settings.KAFKA_TOPIC_TRANSACTIONS,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="fraud_processing_group_v3",
            auto_offset_reset="latest",
            enable_auto_commit=False,
        )

        await consumer.start()

        emit_log(
            event="kafka_consumer_online",
            topic=settings.KAFKA_TOPIC_TRANSACTIONS,
            group_id="fraud_processing_group_v3",
        )

        async for msg in consumer:
            try:
                await process_transaction(
                    tx_data=msg.value,
                    redis_evaluator=redis_evaluator,
                )

                # Commit offset only after successful ledger persistence.
                await consumer.commit()

                emit_log(
                    event="kafka_offset_committed",
                    topic=msg.topic,
                    partition=msg.partition,
                    offset=msg.offset,
                )

            except Exception as exc:
                # Do not commit offset here.
                # Kafka can redeliver depending on consumer group behaviour.
                emit_log(
                    event="kafka_message_processing_failed",
                    level="error",
                    topic=msg.topic,
                    partition=msg.partition,
                    offset=msg.offset,
                    error=str(exc),
                    action="offset_not_committed",
                )

                await asyncio.sleep(1)

    finally:
        emit_log(event="fraud_engine_shutting_down")

        if consumer is not None:
            await consumer.stop()

        await redis_evaluator.close()

        emit_log(event="fraud_engine_shutdown_complete")


if __name__ == "__main__":
    asyncio.run(consume_events())
