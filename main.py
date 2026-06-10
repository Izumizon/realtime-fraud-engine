import hashlib
import inspect
import json
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Annotated, Any, cast
from uuid import UUID, uuid4

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, Header, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from redis.exceptions import RedisError

from config import settings
from redis_fraud_evaluator import RedisSettings, create_redis_client

app = FastAPI(title=settings.PROJECT_NAME, version="1.0.0")

producer: AIOKafkaProducer | None = None
redis_client: Redis | None = None

IDEMPOTENCY_TTL_SECONDS = 60 * 60 * 24  # 24 hours

reserve_idempotency_sha: str | None = None
complete_idempotency_sha: str | None = None
fail_idempotency_sha: str | None = None


# ---------------------------------------------------------------------------
# Redis Lua scripts for atomic idempotency state transitions
# ---------------------------------------------------------------------------

RESERVE_IDEMPOTENCY_LUA = """
local key = KEYS[1]
local request_hash = ARGV[1]
local ttl_seconds = tonumber(ARGV[2])
local now = ARGV[3]

if redis.call("EXISTS", key) == 0 then
    redis.call(
        "HSET",
        key,
        "state", "PROCESSING",
        "request_hash", request_hash,
        "created_at", now,
        "updated_at", now
    )
    redis.call("EXPIRE", key, ttl_seconds)
    return {"RESERVED", ""}
end

local existing_hash = redis.call("HGET", key, "request_hash")

if existing_hash ~= request_hash then
    return {"CONFLICT", ""}
end

local state = redis.call("HGET", key, "state")

if state == "COMPLETED" then
    local response_json = redis.call("HGET", key, "response_json") or ""
    return {"COMPLETED", response_json}
end

if state == "PROCESSING" then
    return {"PROCESSING", ""}
end

if state == "FAILED" then
    redis.call(
        "HSET",
        key,
        "state", "PROCESSING",
        "updated_at", now
    )
    redis.call("EXPIRE", key, ttl_seconds)
    return {"RESERVED", ""}
end

return {"UNKNOWN", ""}
"""


COMPLETE_IDEMPOTENCY_LUA = """
local key = KEYS[1]
local request_hash = ARGV[1]
local response_json = ARGV[2]
local ttl_seconds = tonumber(ARGV[3])
local now = ARGV[4]

if redis.call("EXISTS", key) == 0 then
    return {"NO_KEY", ""}
end

local existing_hash = redis.call("HGET", key, "request_hash")

if existing_hash ~= request_hash then
    return {"CONFLICT", ""}
end

local state = redis.call("HGET", key, "state")

if state ~= "PROCESSING" then
    return {state, ""}
end

redis.call(
    "HSET",
    key,
    "state", "COMPLETED",
    "response_json", response_json,
    "updated_at", now
)

redis.call("EXPIRE", key, ttl_seconds)

return {"COMPLETED", ""}
"""


FAIL_IDEMPOTENCY_LUA = """
local key = KEYS[1]
local request_hash = ARGV[1]
local error_message = ARGV[2]
local ttl_seconds = tonumber(ARGV[3])
local now = ARGV[4]

if redis.call("EXISTS", key) == 0 then
    return {"NO_KEY", ""}
end

local existing_hash = redis.call("HGET", key, "request_hash")

if existing_hash ~= request_hash then
    return {"CONFLICT", ""}
end

redis.call(
    "HSET",
    key,
    "state", "FAILED",
    "error", error_message,
    "updated_at", now
)

redis.call("EXPIRE", key, ttl_seconds)

return {"FAILED", ""}
"""


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
async def load_redis_script(redis: Redis, script: str) -> str:
    """
    redis.asyncio script_load works asynchronously at runtime, but its type
    hints can be broader than mypy expects. This helper normalises the result.
    """

    result = redis.script_load(script)

    if inspect.isawaitable(result):
        return cast(str, await result)

    return cast(str, result)


def now_utc_iso() -> str:
    return datetime.now(UTC).isoformat()


def build_request_hash(payload: dict) -> str:
    """
    Build a deterministic hash of the request body.

    This lets us detect dangerous misuse:
    same Idempotency-Key + different payload = 409 Conflict.
    """

    canonical_json = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def idempotency_redis_key(idempotency_key: UUID) -> str:
    return f"idem:{str(idempotency_key)}"


async def eval_redis_script(
    redis: Redis,
    sha: str,
    num_keys: int,
    *keys_and_args: str,
) -> Any:
    """
    Execute a Redis Lua script and normalise redis-py's broad return typing.

    redis.asyncio evalsha works asynchronously at runtime, but the type hints
    may describe the result as either Awaitable[...] or a direct value.
    """

    result = redis.evalsha(sha, num_keys, *keys_and_args)

    if inspect.isawaitable(result):
        return await cast(Awaitable[Any], result)

    return result


async def reserve_idempotency_key(
    *,
    key: str,
    request_hash: str,
) -> tuple[str, str]:
    """
    Atomically reserve or inspect an idempotency key.

    Returns:
    - RESERVED
    - COMPLETED
    - PROCESSING
    - CONFLICT
    """

    if redis_client is None or reserve_idempotency_sha is None:
        raise RuntimeError("Redis idempotency layer is not initialized")

    result = await eval_redis_script(
        redis_client,
        reserve_idempotency_sha,
        1,
        key,
        request_hash,
        str(IDEMPOTENCY_TTL_SECONDS),
        now_utc_iso(),
    )

    return str(result[0]), str(result[1])


async def mark_idempotency_completed(
    *,
    key: str,
    request_hash: str,
    response_payload: dict,
) -> None:
    """
    Cache the successful API response.

    Future identical requests with the same Idempotency-Key receive this response
    instead of publishing another Kafka event.
    """

    if redis_client is None or complete_idempotency_sha is None:
        raise RuntimeError("Redis idempotency layer is not initialized")

    response_json = json.dumps(response_payload, separators=(",", ":"))

    result = await eval_redis_script(
        redis_client,
        complete_idempotency_sha,
        1,
        key,
        request_hash,
        response_json,
        str(IDEMPOTENCY_TTL_SECONDS),
        now_utc_iso(),
    )

    state = str(result[0])

    if state != "COMPLETED":
        raise RuntimeError(f"Failed to complete idempotency state: {state}")


async def mark_idempotency_failed(
    *,
    key: str,
    request_hash: str,
    error_message: str,
) -> None:
    """
    Mark the state as FAILED after an internal exception.

    This allows future retries to reserve the key again instead of being stuck
    forever in PROCESSING.
    """

    if redis_client is None or fail_idempotency_sha is None:
        return

    await eval_redis_script(
        redis_client,
        fail_idempotency_sha,
        1,
        key,
        request_hash,
        error_message[:250],
        str(IDEMPOTENCY_TTL_SECONDS),
        now_utc_iso(),
    )


# ---------------------------------------------------------------------------
# FastAPI lifecycle
# ---------------------------------------------------------------------------


@app.on_event("startup")
async def startup_event() -> None:
    global producer
    global redis_client
    global reserve_idempotency_sha
    global complete_idempotency_sha
    global fail_idempotency_sha

    redis_client = create_redis_client(
        RedisSettings(
            url=settings.REDIS_URL,
            max_connections=50,
            socket_timeout_seconds=1.0,
            socket_connect_timeout_seconds=2.0,
        )
    )

    reserve_idempotency_sha = await load_redis_script(
        redis_client,
        RESERVE_IDEMPOTENCY_LUA,
    )
    complete_idempotency_sha = await load_redis_script(
        redis_client,
        COMPLETE_IDEMPOTENCY_LUA,
    )
    fail_idempotency_sha = await load_redis_script(
        redis_client,
        FAIL_IDEMPOTENCY_LUA,
    )
    print("✅ Redis Idempotency Layer Online.")

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    await producer.start()
    print("✅ Kafka Gateway Online.")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    global producer
    global redis_client

    if producer:
        await producer.stop()

    if redis_client:
        await redis_client.aclose()
        await redis_client.connection_pool.disconnect()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.post("/api/v1/transactions", status_code=status.HTTP_201_CREATED)
async def receive_transaction(
    tx: Transaction,
    idempotency_key: Annotated[UUID, Header(alias="Idempotency-Key")],
    x_trace_id: Annotated[str | None, Header(alias="X-Trace-Id")] = None,
):
    """
    Receive a transaction, enforce API-level idempotency, and publish to Kafka.

    This protects against:
    - user double-clicking Pay Now
    - client retries
    - merchant timeout retries
    - repeated identical requests
    """

    if producer is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka producer is not ready",
        )

    request_body = tx.model_dump()
    request_hash = build_request_hash(request_body)
    redis_key = idempotency_redis_key(idempotency_key)

    try:
        reservation_state, cached_response = await reserve_idempotency_key(
            key=redis_key,
            request_hash=request_hash,
        )

        if reservation_state == "COMPLETED":
            try:
                return JSONResponse(
                    status_code=status.HTTP_201_CREATED,
                    content=json.loads(cached_response),
                )
            except json.JSONDecodeError as exc:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Cached idempotency response is corrupted",
                ) from exc

        if reservation_state == "PROCESSING":
            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content={
                    "status": "PROCESSING",
                    "message": "Transaction is already being processed",
                },
            )

        if reservation_state == "CONFLICT":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency-Key reused with a different request payload",
            )

        if reservation_state != "RESERVED":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Unexpected idempotency state: {reservation_state}",
            )

        trace_id = x_trace_id or str(uuid4())

        transaction_data = request_body
        transaction_data["idempotency_key"] = str(idempotency_key)
        transaction_data["trace_id"] = trace_id
        transaction_data["received_at"] = now_utc_iso()

        await producer.send_and_wait(
            topic=settings.KAFKA_TOPIC_TRANSACTIONS,
            value=transaction_data,
        )

        response_payload = {
            "status": "QUEUED",
            "transaction_id": tx.transaction_id,
            "trace_id": trace_id,
        }

        await mark_idempotency_completed(
            key=redis_key,
            request_hash=request_hash,
            response_payload=response_payload,
        )

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content=response_payload,
        )

    except HTTPException:
        raise

    except RedisError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Idempotency layer unavailable: {exc}",
        ) from exc

    except Exception as exc:
        await mark_idempotency_failed(
            key=redis_key,
            request_hash=request_hash,
            error_message=str(exc),
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc
