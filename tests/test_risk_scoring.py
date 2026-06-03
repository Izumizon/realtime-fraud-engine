from consumer import calculate_static_risk_score, route_transaction


def test_new_payee_adds_20_risk_points():
    tx_data = {
        "behavioral_metadata": {
            "is_new_payee": True,
            "password_reset_24h": False,
            "time_to_complete_ms": 60_000,
        }
    }

    score, reasons = calculate_static_risk_score(tx_data)

    assert score == 20
    assert "new_payee" in reasons


def test_recent_password_reset_adds_30_risk_points():
    tx_data = {
        "behavioral_metadata": {
            "is_new_payee": False,
            "password_reset_24h": True,
            "time_to_complete_ms": 60_000,
        }
    }

    score, reasons = calculate_static_risk_score(tx_data)

    assert score == 30
    assert "recent_password_reset" in reasons


def test_panic_execution_speed_adds_40_risk_points():
    tx_data = {
        "behavioral_metadata": {
            "is_new_payee": False,
            "password_reset_24h": False,
            "time_to_complete_ms": 1_200,
        }
    }

    score, reasons = calculate_static_risk_score(tx_data)

    assert score == 40
    assert "panic_execution_speed" in reasons


def test_combined_static_risk_is_capped_at_100():
    tx_data = {
        "behavioral_metadata": {
            "is_new_payee": True,
            "password_reset_24h": True,
            "time_to_complete_ms": 1_200,
        }
    }

    score, reasons = calculate_static_risk_score(tx_data)

    assert score == 90
    assert reasons == [
        "new_payee",
        "recent_password_reset",
        "panic_execution_speed",
    ]


def test_missing_behavioral_metadata_defaults_to_zero_risk():
    tx_data = {}

    score, reasons = calculate_static_risk_score(tx_data)

    assert score == 0
    assert reasons == []


def test_route_approved_boundary():
    assert route_transaction(0) == "APPROVED"
    assert route_transaction(39) == "APPROVED"


def test_route_step_up_review_boundary():
    assert route_transaction(40) == "STEP-UP_REVIEW"
    assert route_transaction(69) == "STEP-UP_REVIEW"


def test_route_declined_boundary():
    assert route_transaction(70) == "DECLINED"
    assert route_transaction(100) == "DECLINED"