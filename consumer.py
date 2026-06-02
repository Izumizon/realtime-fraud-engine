import asyncio
import json
from typing import Any
from uuid import UUID

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


async def process_transaction(
    tx_data: dict[str, Any],
    redis_evaluator: RedisFraudEvaluator,
) -> None:
    """
    Full transaction evaluation pipeline.

    Flow:
    1. Calculate deterministic APP scam risk.
    2. Evaluate Redis dynamic risk:
       - sender velocity
       - receiver swarm
    3. Route the final decision.
    4. Persist to PostgreSQL ledger.
    """

    static_score, static_reasons = calculate_static_risk_score(tx_data)

    redis_reasons: list[str] = []
    redis_eval_ms = 0.0

    try:
        redis_input = TransactionRiskInput(
            transaction_id=UUID(tx_data["transaction_id"]),
            user_id=tx_data["user_id"],
            merchant_id=tx_data["merchant_id"],
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

    except RedisError as exc:
        # Redis failure should not crash the consumer.
        # In the cold-path consumer, we degrade to static-only scoring.
        final_score = static_score
        redis_reasons = ["redis_unavailable_static_only"]
        print(f"⚠️ Redis unavailable during evaluation: {exc}")

    except Exception as exc:
        # Malformed payloads should be visible and should not silently poison
        # the stream. In a larger system this would go to a DLQ.
        raise RuntimeError(f"Failed to evaluate transaction payload: {exc}") from exc

    final_status = route_transaction(final_score)

    all_reasons = static_reasons + redis_reasons

    if final_status == "DECLINED":
        print(
            f"🚫 [BLOCKED] Tx={tx_data['transaction_id']} "
            f"User={tx_data['user_id']} "
            f"Merchant={tx_data['merchant_id']} "
            f"Score={final_score} "
            f"Reasons={all_reasons} "
            f"RedisEval={redis_eval_ms:.2f}ms"
        )

    elif final_status == "STEP-UP_REVIEW":
        print(
            f"⚠️ [REVIEW] Tx={tx_data['transaction_id']} "
            f"User={tx_data['user_id']} "
            f"Merchant={tx_data['merchant_id']} "
            f"Score={final_score} "
            f"Reasons={all_reasons} "
            f"RedisEval={redis_eval_ms:.2f}ms"
        )

    else:
        print(
            f"✅ [APPROVED] Tx={tx_data['transaction_id']} "
            f"User={tx_data['user_id']} "
            f"Merchant={tx_data['merchant_id']} "
            f"Score={final_score} "
            f"Reasons={all_reasons} "
            f"RedisEval={redis_eval_ms:.2f}ms"
        )

    await save_transaction_to_ledger(
        tx_data=tx_data,
        final_status=final_status,
        risk_score=final_score,
    )

async def consume_events() -> None:
    """
    Main Kafka consumer loop.

    Startup order:
    1. Start Redis evaluator first.
    2. Only create/start Kafka consumer after Redis is ready.
    3. Commit Kafka offset only after PostgreSQL persistence succeeds.
    """

    print("🧠 Fraud Engine Brain waking up...")
    print(f"🔗 Connecting to Redis at {settings.REDIS_URL}")

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
        print("✅ Redis Intelligence Layer online.")

        print(f"🔗 Connecting to Kafka at {settings.KAFKA_BOOTSTRAP_SERVERS}")

        consumer = AIOKafkaConsumer(
            settings.KAFKA_TOPIC_TRANSACTIONS,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="fraud_processing_group_v3",
            auto_offset_reset="latest",
            enable_auto_commit=False,
        )

        await consumer.start()
        print("✅ Brain is online: Kafka + Redis Intelligence Layer connected.")

        async for msg in consumer:
            try:
                await process_transaction(
                    tx_data=msg.value,
                    redis_evaluator=redis_evaluator,
                )

                await consumer.commit()

            except Exception as exc:
                print(
                    f"❌ Failed to process Kafka message. "
                    f"Offset NOT committed. Error={exc}"
                )
                await asyncio.sleep(1)

    finally:
        print("🛑 Shutting down Fraud Engine Brain...")

        if consumer is not None:
            await consumer.stop()

        await redis_evaluator.close()

        print("✅ Shutdown complete.")

if __name__ == "__main__":
    asyncio.run(consume_events())