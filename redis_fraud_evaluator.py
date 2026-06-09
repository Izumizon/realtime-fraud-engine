"""
redis_fraud_evaluator.py

Async Redis Intelligence Layer for real-time fraud scoring.

Implements:
1. Sender Velocity:
   - Key: vel:user:{user_id}
   - Counts unique transaction attempts from one sender in the last 10 minutes.

2. Receiver Swarm:
   - Key: vel:merch:{merchant_id}
   - Counts unique user_ids interacting with one merchant in the last 10 minutes.

Design goals:
- Async-first for FastAPI / aiokafka event loops.
- Atomic per-key sliding-window evaluation using Redis Lua scripts.
- No KEYS, SCAN, blocking operations, or heavy global queries.
- P99 target: Redis evaluation should remain under ~10ms under bounded cardinality.
"""

from __future__ import annotations

import time
from uuid import UUID

from pydantic import BaseModel, Field
from redis.asyncio import ConnectionPool, Redis
from redis.exceptions import NoScriptError

# ---------------------------------------------------------------------------
# Lua script: atomic sliding-window counter using a Redis Sorted Set.
#
# KEYS[1] = Redis ZSET key
#
# ARGV[1] = now_ms
# ARGV[2] = window_ms
# ARGV[3] = member
# ARGV[4] = ttl_seconds
#
# Behaviour:
# 1. Remove entries older than the sliding window.
# 2. Add/update the current member using timestamp as score.
# 3. Count remaining members in the active window.
# 4. Refresh TTL so idle windows naturally expire.
#
# Important:
# - For sender velocity, member should be transaction_id.
# - For merchant swarm, member should be user_id.
#
# Why ZADD GT?
# - Prevents older replayed events from moving an existing member backwards
#   in time. If your Redis version does not support ZADD GT, remove "GT".
# ---------------------------------------------------------------------------

SLIDING_WINDOW_ZSET_LUA = """
local key = KEYS[1]

local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local member = ARGV[3]
local ttl_seconds = tonumber(ARGV[4])

if now_ms == nil then
    return redis.error_reply("now_ms must be numeric")
end

if window_ms == nil then
    return redis.error_reply("window_ms must be numeric")
end

if ttl_seconds == nil then
    return redis.error_reply("ttl_seconds must be numeric")
end

local cutoff_ms = now_ms - window_ms

-- Remove expired entries first.
redis.call("ZREMRANGEBYSCORE", key, "-inf", cutoff_ms)

-- Add or update the current member.
-- GT ensures an older replay cannot move an existing member backwards.
redis.call("ZADD", key, "GT", now_ms, member)

-- Count active entries in the sliding window.
local count = redis.call("ZCARD", key)

-- Expire idle windows automatically.
redis.call("EXPIRE", key, ttl_seconds)

return {count, now_ms}
"""


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class RedisSettings(BaseModel):
    """
    Redis connection configuration.

    Important:
    socket_timeout_seconds is not the latency target.
    It is the failure threshold.

    A 10ms timeout is too aggressive for Windows + Docker startup,
    even if normal Redis evaluation later runs in 1-5ms.
    """

    url: str = Field(default="redis://localhost:6379/0")
    max_connections: int = Field(default=50, ge=1)

    # Production target can still be <10ms P99.
    # But connection-level timeouts should allow Docker/network jitter.
    socket_timeout_seconds: float = Field(default=1.0, gt=0)
    socket_connect_timeout_seconds: float = Field(default=2.0, gt=0)

    health_check_interval_seconds: int = Field(default=30, ge=0)


class FraudRuleConfig(BaseModel):
    """
    Dynamic fraud threshold configuration.

    These values can later be moved behind a remote config service.
    """

    window_seconds: int = Field(default=600, ge=1)  # 10 minutes

    sender_velocity_threshold: int = Field(default=5, ge=1)
    receiver_swarm_threshold: int = Field(default=10, ge=1)

    sender_velocity_penalty: int = Field(default=35, ge=0, le=100)
    receiver_swarm_penalty: int = Field(default=45, ge=0, le=100)

    # Extra TTL buffer prevents immediate key expiry exactly at window boundary.
    ttl_grace_seconds: int = Field(default=60, ge=0)


class TransactionRiskInput(BaseModel):
    """
    Minimal transaction shape needed by the Redis intelligence layer.

    transaction_id:
        Used as the unique member for sender velocity tracking.
        This avoids counting idempotent retries as separate attempts.

    user_id:
        Used as the unique sender identifier.

    merchant_id:
        Used as the receiver / merchant identifier.

    amount:
        Integer minor units only.
        Example: £10.50 = 1050.
    """

    transaction_id: UUID
    user_id: str = Field(min_length=1)
    merchant_id: str = Field(min_length=1)
    amount: int = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)


class FraudRiskResult(BaseModel):
    """
    Result returned by RedisFraudEvaluator.
    """

    risk_score: int = Field(ge=0, le=100)

    sender_velocity_count: int = Field(ge=0)
    receiver_unique_sender_count: int = Field(ge=0)

    sender_velocity_penalty_applied: int = Field(ge=0, le=100)
    receiver_swarm_penalty_applied: int = Field(ge=0, le=100)

    reasons: list[str]
    redis_eval_time_ms: float = Field(ge=0)


# ---------------------------------------------------------------------------
# Redis connection factory
# ---------------------------------------------------------------------------


def create_redis_client(settings: RedisSettings) -> Redis:
    """
    Create an async Redis client backed by a connection pool.

    This is safe to create once during FastAPI startup and reuse across requests
    or Kafka consumer messages.
    """

    pool = ConnectionPool.from_url(
        settings.url,
        max_connections=settings.max_connections,
        socket_timeout=settings.socket_timeout_seconds,
        socket_connect_timeout=settings.socket_connect_timeout_seconds,
        health_check_interval=settings.health_check_interval_seconds,
        decode_responses=True,
    )

    return Redis(connection_pool=pool)


# ---------------------------------------------------------------------------
# Redis fraud evaluator
# ---------------------------------------------------------------------------


class RedisFraudEvaluator:
    """
    Async Redis-backed fraud scoring layer.

    Atomicity model:
    - Each sliding-window update is atomic per Redis key because it is executed
      inside a Lua script.
    - Sender velocity and receiver swarm are evaluated as two separate atomic
      key operations.
    - This avoids cross-slot Lua issues in Redis Cluster while preserving
      correctness per fraud dimension.
    """

    def __init__(
        self,
        redis_client: Redis,
        config: FraudRuleConfig | None = None,
    ) -> None:
        self.redis = redis_client
        self.config = config or FraudRuleConfig()
        self._sliding_window_sha: str | None = None

    async def start(self) -> None:
        """
        Load Lua scripts into Redis.

        Call this once during application startup.
        """

        self._sliding_window_sha = await self.redis.script_load(SLIDING_WINDOW_ZSET_LUA)

    async def close(self) -> None:
        """
        Close Redis connections cleanly.

        Important for FastAPI lifespan shutdown and test isolation.
        """

        await self.redis.aclose()
        await self.redis.connection_pool.disconnect()

    async def evaluate_transaction(
        self,
        transaction: TransactionRiskInput,
        base_risk_score: int = 0,
    ) -> FraudRiskResult:
        """
        Evaluate real-time dynamic risk for one transaction.

        Applies:
        1. Sender velocity penalty if user exceeds threshold.
        2. Receiver swarm penalty if merchant receives money from too many
           unique users in the window.

        Returns:
            FraudRiskResult with modified 0-100 risk score.
        """

        if self._sliding_window_sha is None:
            await self.start()

        base_risk_score = self._clamp_score(base_risk_score)

        started_ns = time.perf_counter_ns()

        now_ms = int(time.time() * 1000)
        window_ms = self.config.window_seconds * 1000
        ttl_seconds = self.config.window_seconds + self.config.ttl_grace_seconds

        sender_key = f"vel:user:{transaction.user_id}"
        receiver_key = f"vel:merch:{transaction.merchant_id}"

        # Sender velocity counts unique transaction attempts.
        sender_member = str(transaction.transaction_id)

        # Receiver swarm counts unique users interacting with the merchant.
        receiver_member = transaction.user_id

        try:
            sender_count, receiver_unique_count = await self._run_window_checks(
                sender_key=sender_key,
                sender_member=sender_member,
                receiver_key=receiver_key,
                receiver_member=receiver_member,
                now_ms=now_ms,
                window_ms=window_ms,
                ttl_seconds=ttl_seconds,
            )
        except NoScriptError:
            # Redis can evict script cache after SCRIPT FLUSH or failover.
            # Reload once and retry.
            await self.start()
            sender_count, receiver_unique_count = await self._run_window_checks(
                sender_key=sender_key,
                sender_member=sender_member,
                receiver_key=receiver_key,
                receiver_member=receiver_member,
                now_ms=now_ms,
                window_ms=window_ms,
                ttl_seconds=ttl_seconds,
            )

        reasons: list[str] = []

        sender_penalty = 0
        if sender_count > self.config.sender_velocity_threshold:
            sender_penalty = self.config.sender_velocity_penalty
            reasons.append("sender_velocity_exceeded")

        receiver_penalty = 0
        if receiver_unique_count > self.config.receiver_swarm_threshold:
            receiver_penalty = self.config.receiver_swarm_penalty
            reasons.append("receiver_swarm_detected")

        risk_score = self._clamp_score(
            base_risk_score + sender_penalty + receiver_penalty
        )

        elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000

        return FraudRiskResult(
            risk_score=risk_score,
            sender_velocity_count=sender_count,
            receiver_unique_sender_count=receiver_unique_count,
            sender_velocity_penalty_applied=sender_penalty,
            receiver_swarm_penalty_applied=receiver_penalty,
            reasons=reasons,
            redis_eval_time_ms=elapsed_ms,
        )

    async def _run_window_checks(
        self,
        *,
        sender_key: str,
        sender_member: str,
        receiver_key: str,
        receiver_member: str,
        now_ms: int,
        window_ms: int,
        ttl_seconds: int,
    ) -> tuple[int, int]:
        """
        Execute both sliding-window evaluations.

        Uses a non-transactional pipeline to reduce round trips.
        Each Lua script remains atomic for its own key.
        """

        assert self._sliding_window_sha is not None

        pipe = self.redis.pipeline(transaction=False)

        pipe.evalsha(
            self._sliding_window_sha,
            1,
            sender_key,
            now_ms,
            window_ms,
            sender_member,
            ttl_seconds,
        )

        pipe.evalsha(
            self._sliding_window_sha,
            1,
            receiver_key,
            now_ms,
            window_ms,
            receiver_member,
            ttl_seconds,
        )

        sender_result, receiver_result = await pipe.execute()

        sender_count = int(sender_result[0])
        receiver_unique_count = int(receiver_result[0])

        return sender_count, receiver_unique_count

    @staticmethod
    def _clamp_score(score: int) -> int:
        """
        Keep risk score within 0-100.
        """

        return max(0, min(100, int(score)))


# ---------------------------------------------------------------------------
# Example usage inside an async Kafka consumer or FastAPI route
# ---------------------------------------------------------------------------


async def example_usage() -> None:
    redis_settings = RedisSettings(
        url="redis://localhost:6379/0",
        max_connections=50,
    )

    redis_client = create_redis_client(redis_settings)

    evaluator = RedisFraudEvaluator(
        redis_client=redis_client,
        config=FraudRuleConfig(
            window_seconds=600,
            sender_velocity_threshold=5,
            receiver_swarm_threshold=10,
            sender_velocity_penalty=35,
            receiver_swarm_penalty=45,
        ),
    )

    await evaluator.start()

    try:
        tx = TransactionRiskInput(
            transaction_id=UUID("550e8400-e29b-41d4-a716-446655440000"),
            user_id="user-123",
            merchant_id="merchant-999",
            amount=1050,
            currency="GBP",
        )

        result = await evaluator.evaluate_transaction(
            transaction=tx,
            base_risk_score=10,
        )

        print(result.model_dump())

    finally:
        await evaluator.close()
