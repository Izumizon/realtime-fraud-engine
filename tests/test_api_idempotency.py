import json
from uuid import uuid4

from fastapi.testclient import TestClient

import main


class FakeKafkaProducer:
    """
    Fake async Kafka producer for API tests.

    This prevents tests from needing a real Kafka broker.
    """

    def __init__(self) -> None:
        self.sent_messages = []

    async def send_and_wait(self, topic: str, value: dict) -> None:
        self.sent_messages.append(
            {
                "topic": topic,
                "value": value,
            }
        )


def valid_transaction_payload(amount: int = 1050) -> dict:
    return {
        "transaction_id": str(uuid4()),
        "user_id": "user_1001",
        "merchant_id": "m-9921",
        "amount": amount,
        "currency": "GBP",
        "device_ip": "192.168.1.1",
        "behavioral_metadata": {
            "time_to_complete_ms": 1200,
            "is_new_payee": True,
            "password_reset_24h": False,
        },
    }


def test_build_request_hash_is_deterministic():
    """
    Same logical payload with different key ordering should produce
    the same hash.
    """

    payload_a = {
        "user_id": "user_1",
        "amount": 1050,
        "currency": "GBP",
    }

    payload_b = {
        "currency": "GBP",
        "amount": 1050,
        "user_id": "user_1",
    }

    assert main.build_request_hash(payload_a) == main.build_request_hash(payload_b)


def test_first_request_reserves_key_and_publishes_to_kafka(monkeypatch):
    """
    First request should reserve the idempotency key, publish exactly one
    Kafka message, mark the key as completed, and return 201.
    """

    fake_producer = FakeKafkaProducer()
    completed_calls = []

    async def fake_reserve_idempotency_key(*, key: str, request_hash: str):
        return "RESERVED", ""

    async def fake_mark_idempotency_completed(
        *,
        key: str,
        request_hash: str,
        response_payload: dict,
    ):
        completed_calls.append(response_payload)

    monkeypatch.setattr(main, "producer", fake_producer)
    monkeypatch.setattr(
        main,
        "reserve_idempotency_key",
        fake_reserve_idempotency_key,
    )
    monkeypatch.setattr(
        main,
        "mark_idempotency_completed",
        fake_mark_idempotency_completed,
    )

    client = TestClient(main.app)

    payload = valid_transaction_payload()
    idempotency_key = str(uuid4())

    response = client.post(
        "/api/v1/transactions",
        headers={"Idempotency-Key": idempotency_key},
        json=payload,
    )

    assert response.status_code == 201

    body = response.json()
    assert body["status"] == "QUEUED"
    assert body["transaction_id"] == payload["transaction_id"]
    assert "trace_id" in body

    assert len(fake_producer.sent_messages) == 1
    assert fake_producer.sent_messages[0]["value"]["idempotency_key"] == idempotency_key
    assert fake_producer.sent_messages[0]["value"]["trace_id"] == body["trace_id"]

    assert completed_calls == [body]


def test_duplicate_completed_request_returns_cached_response(monkeypatch):
    """
    If Redis says the idempotency key is already COMPLETED, the API should
    return the cached response and should not publish to Kafka again.
    """

    fake_producer = FakeKafkaProducer()

    cached_response = {
        "status": "QUEUED",
        "transaction_id": "cached-tx-id",
        "trace_id": "cached-trace-id",
    }

    async def fake_reserve_idempotency_key(*, key: str, request_hash: str):
        return "COMPLETED", json.dumps(cached_response)

    monkeypatch.setattr(main, "producer", fake_producer)
    monkeypatch.setattr(
        main,
        "reserve_idempotency_key",
        fake_reserve_idempotency_key,
    )

    client = TestClient(main.app)

    response = client.post(
        "/api/v1/transactions",
        headers={"Idempotency-Key": str(uuid4())},
        json=valid_transaction_payload(),
    )

    assert response.status_code == 201
    assert response.json() == cached_response
    assert fake_producer.sent_messages == []


def test_duplicate_processing_request_returns_202(monkeypatch):
    """
    If Redis says the request is already PROCESSING, the API should return
    202 Accepted instead of queueing another Kafka event.
    """

    fake_producer = FakeKafkaProducer()

    async def fake_reserve_idempotency_key(*, key: str, request_hash: str):
        return "PROCESSING", ""

    monkeypatch.setattr(main, "producer", fake_producer)
    monkeypatch.setattr(
        main,
        "reserve_idempotency_key",
        fake_reserve_idempotency_key,
    )

    client = TestClient(main.app)

    response = client.post(
        "/api/v1/transactions",
        headers={"Idempotency-Key": str(uuid4())},
        json=valid_transaction_payload(),
    )

    assert response.status_code == 202
    assert response.json()["status"] == "PROCESSING"
    assert fake_producer.sent_messages == []


def test_same_idempotency_key_different_payload_returns_409(monkeypatch):
    """
    If Redis detects the same Idempotency-Key with a different request hash,
    the API should reject it with 409 Conflict.
    """

    fake_producer = FakeKafkaProducer()

    async def fake_reserve_idempotency_key(*, key: str, request_hash: str):
        return "CONFLICT", ""

    monkeypatch.setattr(main, "producer", fake_producer)
    monkeypatch.setattr(
        main,
        "reserve_idempotency_key",
        fake_reserve_idempotency_key,
    )

    client = TestClient(main.app)

    response = client.post(
        "/api/v1/transactions",
        headers={"Idempotency-Key": str(uuid4())},
        json=valid_transaction_payload(amount=9999),
    )

    assert response.status_code == 409
    assert response.json()["detail"] == (
        "Idempotency-Key reused with a different request payload"
    )
    assert fake_producer.sent_messages == []


def test_missing_idempotency_key_returns_422(monkeypatch):
    """
    Idempotency-Key is mandatory. FastAPI should reject the request before
    the endpoint logic runs.
    """

    monkeypatch.setattr(main, "producer", FakeKafkaProducer())

    client = TestClient(main.app)

    response = client.post(
        "/api/v1/transactions",
        json=valid_transaction_payload(),
    )

    assert response.status_code == 422
