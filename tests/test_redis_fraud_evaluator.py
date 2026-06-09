from uuid import uuid4

import pytest

from redis_fraud_evaluator import (
    FraudRuleConfig,
    RedisFraudEvaluator,
    RedisSettings,
    TransactionRiskInput,
    create_redis_client,
)


@pytest.mark.asyncio
async def test_sender_velocity_triggers_after_threshold():
    """
    Proves that one user making too many transaction attempts inside the
    sliding window receives the sender_velocity_exceeded risk reason.
    """

    user_id = f"test_user_{uuid4()}"
    merchant_id = f"test_merchant_{uuid4()}"

    redis_client = create_redis_client(
        RedisSettings(
             url="redis://127.0.0.1:6379/0",
            socket_timeout_seconds=1.0,
            socket_connect_timeout_seconds=2.0,
        )
    )

    evaluator = RedisFraudEvaluator(
        redis_client=redis_client,
        config=FraudRuleConfig(
            window_seconds=600,
            sender_velocity_threshold=2,
            receiver_swarm_threshold=999,
            sender_velocity_penalty=35,
            receiver_swarm_penalty=45,
        ),
    )

    await evaluator.start()

    try:
        result = None

        for _ in range(3):
            tx = TransactionRiskInput(
                transaction_id=uuid4(),
                user_id=user_id,
                merchant_id=merchant_id,
                amount=1050,
                currency="GBP",
            )

            result = await evaluator.evaluate_transaction(
                transaction=tx,
                base_risk_score=0,
            )

        assert result is not None
        assert result.sender_velocity_count == 3
        assert result.risk_score == 35
        assert "sender_velocity_exceeded" in result.reasons

    finally:
        await redis_client.delete(f"vel:user:{user_id}")
        await redis_client.delete(f"vel:merch:{merchant_id}")
        await evaluator.close()


@pytest.mark.asyncio
async def test_receiver_swarm_triggers_after_unique_sender_threshold():
    """
    Proves that many unique users paying the same merchant inside the sliding
    window trigger receiver_swarm_detected.
    """

    merchant_id = f"test_merchant_{uuid4()}"

    redis_client = create_redis_client(
        RedisSettings(
             url="redis://127.0.0.1:6379/0",
            socket_timeout_seconds=1.0,
            socket_connect_timeout_seconds=2.0,
        )
    )

    evaluator = RedisFraudEvaluator(
        redis_client=redis_client,
        config=FraudRuleConfig(
            window_seconds=600,
            sender_velocity_threshold=999,
            receiver_swarm_threshold=2,
            sender_velocity_penalty=35,
            receiver_swarm_penalty=45,
        ),
    )

    await evaluator.start()

    user_ids = [f"test_user_{uuid4()}" for _ in range(3)]

    try:
        result = None

        for user_id in user_ids:
            tx = TransactionRiskInput(
                transaction_id=uuid4(),
                user_id=user_id,
                merchant_id=merchant_id,
                amount=1050,
                currency="GBP",
            )

            result = await evaluator.evaluate_transaction(
                transaction=tx,
                base_risk_score=0,
            )

        assert result is not None
        assert result.receiver_unique_sender_count == 3
        assert result.risk_score == 45
        assert "receiver_swarm_detected" in result.reasons

    finally:
        for user_id in user_ids:
            await redis_client.delete(f"vel:user:{user_id}")

        await redis_client.delete(f"vel:merch:{merchant_id}")
        await evaluator.close()


@pytest.mark.asyncio
async def test_duplicate_transaction_id_does_not_inflate_sender_velocity():
    """
    Proves idempotent retry safety at the Redis intelligence layer.

    If the same transaction_id is evaluated twice for the same user, the ZSET
    member is the same, so the sender velocity count should remain 1.
    """

    user_id = f"test_user_{uuid4()}"
    merchant_id = f"test_merchant_{uuid4()}"
    transaction_id = uuid4()

    redis_client = create_redis_client(
        RedisSettings(
             url="redis://127.0.0.1:6379/0",
            socket_timeout_seconds=1.0,
            socket_connect_timeout_seconds=2.0,
        )
    )

    evaluator = RedisFraudEvaluator(
        redis_client=redis_client,
        config=FraudRuleConfig(
            window_seconds=600,
            sender_velocity_threshold=1,
            receiver_swarm_threshold=999,
            sender_velocity_penalty=35,
            receiver_swarm_penalty=45,
        ),
    )

    await evaluator.start()

    try:
        tx = TransactionRiskInput(
            transaction_id=transaction_id,
            user_id=user_id,
            merchant_id=merchant_id,
            amount=1050,
            currency="GBP",
        )

        first_result = await evaluator.evaluate_transaction(
            transaction=tx,
            base_risk_score=0,
        )

        second_result = await evaluator.evaluate_transaction(
            transaction=tx,
            base_risk_score=0,
        )

        assert first_result.sender_velocity_count == 1
        assert second_result.sender_velocity_count == 1
        assert "sender_velocity_exceeded" not in second_result.reasons

    finally:
        await redis_client.delete(f"vel:user:{user_id}")
        await redis_client.delete(f"vel:merch:{merchant_id}")
        await evaluator.close()


@pytest.mark.asyncio
async def test_redis_penalty_is_added_to_existing_base_risk_score():
    """
    Proves Redis dynamic risk combines with existing APP scam/static risk.
    """

    user_id = f"test_user_{uuid4()}"
    merchant_id = f"test_merchant_{uuid4()}"

    redis_client = create_redis_client(
        RedisSettings(
             url="redis://127.0.0.1:6379/0",
            socket_timeout_seconds=1.0,
            socket_connect_timeout_seconds=2.0,
        )
    )

    evaluator = RedisFraudEvaluator(
        redis_client=redis_client,
        config=FraudRuleConfig(
            window_seconds=600,
            sender_velocity_threshold=1,
            receiver_swarm_threshold=999,
            sender_velocity_penalty=35,
            receiver_swarm_penalty=45,
        ),
    )

    await evaluator.start()

    try:
        result = None

        for _ in range(2):
            tx = TransactionRiskInput(
                transaction_id=uuid4(),
                user_id=user_id,
                merchant_id=merchant_id,
                amount=1050,
                currency="GBP",
            )

            result = await evaluator.evaluate_transaction(
                transaction=tx,
                base_risk_score=40,
            )

        assert result is not None
        assert result.sender_velocity_count == 2
        assert result.risk_score == 75
        assert "sender_velocity_exceeded" in result.reasons

    finally:
        await redis_client.delete(f"vel:user:{user_id}")
        await redis_client.delete(f"vel:merch:{merchant_id}")
        await evaluator.close()