from datetime import datetime
from types import SimpleNamespace
from typing import Any

from fastapi.testclient import TestClient
from pytest import MonkeyPatch

import dashboard


class FakeMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)

    def one_or_none(self) -> dict[str, Any] | None:
        if not self.rows:
            return None

        return self.rows[0]


class FakeResult:
    def __init__(
        self,
        *,
        mappings: list[dict[str, Any]] | None = None,
        one: Any | None = None,
        rows: list[Any] | None = None,
    ) -> None:
        self._mappings = mappings or []
        self._one = one
        self._rows = rows or []

    def mappings(self) -> FakeMappings:
        return FakeMappings(self._mappings)

    def one(self) -> Any:
        return self._one

    def __iter__(self):
        return iter(self._rows)


class FakeSession:
    def __init__(self, results: list[FakeResult]) -> None:
        self.results = results
        self.index = 0

    async def __aenter__(self) -> "FakeSession":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(self, *_args: object, **_kwargs: object) -> FakeResult:
        result = self.results[self.index]
        self.index += 1
        return result


def build_fake_results() -> list[FakeResult]:
    return [
        FakeResult(
            mappings=[
                {"status": "APPROVED", "row_count": 7},
                {"status": "STEP-UP_REVIEW", "row_count": 2},
                {"status": "DECLINED", "row_count": 1},
            ]
        ),
        FakeResult(
            one=SimpleNamespace(
                total_volume=10,
                average_risk_score=24.5,
            )
        ),
        FakeResult(
            one=SimpleNamespace(
                total_volume=100,
            )
        ),
        FakeResult(
            mappings=[
                {"risk_band": "low", "row_count": 7},
                {"risk_band": "medium", "row_count": 2},
                {"risk_band": "high", "row_count": 1},
            ]
        ),
        FakeResult(
            mappings=[
                {"reason": "new_payee", "row_count": 3},
                {"reason": "receiver_swarm_detected", "row_count": 2},
            ]
        ),
        FakeResult(
            rows=[
                SimpleNamespace(
                    transaction_id="tx-1",
                    trace_id="trace-1",
                    user_id="user-1",
                    merchant_id="merchant-1",
                    amount=1050,
                    currency="GBP",
                    status="APPROVED",
                    risk_score=20,
                    risk_reasons=["new_payee"],
                    redis_eval_ms=0.5,
                    sender_velocity_count=1,
                    receiver_unique_sender_count=2,
                    received_at=datetime(2026, 6, 10, 12, 0, 0),
                )
            ]
        ),
    ]


def install_fake_database(monkeypatch) -> None:
    def fake_session_factory() -> FakeSession:
        return FakeSession(build_fake_results())

    monkeypatch.setattr(dashboard, "AsyncSessionLocal", fake_session_factory)


def test_dashboard_home_loads() -> None:
    with TestClient(dashboard.app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert "Fraud Operations Dashboard" in response.text


def test_dashboard_stats_accepts_valid_windows(monkeypatch) -> None:
    for window in ["5m", "1h", "24h", "all"]:
        install_fake_database(monkeypatch)

        with TestClient(dashboard.app) as client:
            response = client.get(f"/api/stats?window={window}")

        assert response.status_code == 200

        data = response.json()

        assert data["window"] == window
        assert "window_label" in data
        assert data["total_volume"] == 10
        assert data["all_time_volume"] == 100
        assert data["approved"] == 7
        assert data["step_up_review"] == 2
        assert data["declined"] == 1
        assert data["average_risk_score"] == 24.5
        assert "decision_percentages" in data
        assert "risk_breakdown" in data
        assert "top_fraud_vectors" in data
        assert "latest_transactions" in data


def test_dashboard_stats_invalid_window_defaults_to_one_hour(monkeypatch) -> None:
    install_fake_database(monkeypatch)

    with TestClient(dashboard.app) as client:
        response = client.get("/api/stats?window=invalid")

    assert response.status_code == 200

    data = response.json()

    assert data["window"] == "1h"
    assert data["window_label"] == "Last 1 hour"


def test_transaction_detail_page_loads() -> None:
    with TestClient(dashboard.app) as client:
        response = client.get("/transactions/trace-1")

    assert response.status_code == 200
    assert "Transaction Detail" in response.text
    assert "trace-1" in response.text


def test_transaction_detail_api_returns_transaction(monkeypatch: MonkeyPatch) -> None:
    def fake_session_factory() -> FakeSession:
        return FakeSession(
            [
                FakeResult(
                    mappings=[
                        {
                            "transaction_id": "tx-1",
                            "trace_id": "trace-1",
                            "user_id": "user-1",
                            "merchant_id": "merchant-1",
                            "amount": 1050,
                            "currency": "GBP",
                            "status": "APPROVED",
                            "risk_score": 20,
                            "risk_reasons": ["new_payee"],
                            "redis_eval_ms": 0.5,
                            "sender_velocity_count": 1,
                            "receiver_unique_sender_count": 2,
                            "received_at": datetime(2026, 6, 10, 12, 0, 0),
                        }
                    ]
                )
            ]
        )

    monkeypatch.setattr(dashboard, "AsyncSessionLocal", fake_session_factory)

    with TestClient(dashboard.app) as client:
        response = client.get("/api/transactions/trace-1")

    assert response.status_code == 200

    data = response.json()

    assert data["trace_id"] == "trace-1"
    assert data["transaction_id"] == "tx-1"
    assert data["risk_reasons"] == ["new_payee"]


def test_transaction_detail_api_returns_404(monkeypatch: MonkeyPatch) -> None:
    def fake_session_factory() -> FakeSession:
        return FakeSession([FakeResult(mappings=[])])

    monkeypatch.setattr(dashboard, "AsyncSessionLocal", fake_session_factory)

    with TestClient(dashboard.app) as client:
        response = client.get("/api/transactions/missing-trace")

    assert response.status_code == 404
