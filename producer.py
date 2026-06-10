import asyncio
import json
import random
import uuid
from typing import Any

from aiokafka import AIOKafkaProducer

from config import settings

NORMAL_USERS = [f"user_{i}" for i in range(1000, 5000)]
NORMAL_MERCHANTS = [f"m-{i}" for i in range(1000, 1200)]

MULE_MERCHANT = "m-mule-sink-001"
BOT_USER = "user_bot_velocity_001"

CURRENCIES = ["GBP", "EUR", "USD"]


def pounds_to_minor_units(value: float) -> int:
    """
    Convert a decimal currency amount to integer minor units.
    Example: 10.50 -> 1050
    """
    return int(round(value * 100))


def build_transaction(
    *,
    user_id: str,
    merchant_id: str,
    amount_major: float,
    is_new_payee: bool,
    password_reset_24h: bool,
    time_to_complete_ms: int,
) -> dict[str, Any]:
    return {
        "transaction_id": str(uuid.uuid4()),
        "trace_id": str(uuid.uuid4()),
        "user_id": user_id,
        "merchant_id": merchant_id,
        "amount": pounds_to_minor_units(amount_major),
        "currency": random.choice(CURRENCIES),
        "device_ip": f"192.168.1.{random.randint(1, 255)}",
        "behavioral_metadata": {
            "time_to_complete_ms": time_to_complete_ms,
            "is_new_payee": is_new_payee,
            "password_reset_24h": password_reset_24h,
        },
    }


def build_normal_transaction() -> dict[str, Any]:
    return build_transaction(
        user_id=random.choice(NORMAL_USERS),
        merchant_id=random.choice(NORMAL_MERCHANTS),
        amount_major=random.uniform(5.0, 150.0),
        is_new_payee=random.random() < 0.15,
        password_reset_24h=False,
        time_to_complete_ms=random.randint(15_000, 60_000),
    )


def build_app_scam_transaction() -> dict[str, Any]:
    return build_transaction(
        user_id=random.choice(NORMAL_USERS),
        merchant_id=random.choice(["m-crypto-001", "m-transfer-002", "m-safe-account-003"]),
        amount_major=random.uniform(500.0, 3500.0),
        is_new_payee=True,
        password_reset_24h=True,
        time_to_complete_ms=random.randint(1_200, 2_900),
    )


def build_mule_swarm_transaction() -> dict[str, Any]:
    return build_transaction(
        user_id=random.choice(NORMAL_USERS),
        merchant_id=MULE_MERCHANT,
        amount_major=random.uniform(20.0, 200.0),
        is_new_payee=random.random() < 0.35,
        password_reset_24h=False,
        time_to_complete_ms=random.randint(8_000, 45_000),
    )


def build_bot_velocity_transaction() -> dict[str, Any]:
    return build_transaction(
        user_id=BOT_USER,
        merchant_id=random.choice(NORMAL_MERCHANTS),
        amount_major=random.uniform(10.0, 120.0),
        is_new_payee=False,
        password_reset_24h=False,
        time_to_complete_ms=random.randint(800, 2_500),
    )


async def simulate_transactions() -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    await producer.start()
    print("🚀 Simulator streaming calibrated financial activity...")

    try:
        while True:
            roll = random.random()

            if roll < 0.78:
                scenario = "NORMAL"
                payload = build_normal_transaction()
            elif roll < 0.88:
                scenario = "APP_SCAM"
                payload = build_app_scam_transaction()
            elif roll < 0.97:
                scenario = "MULE_SWARM"
                payload = build_mule_swarm_transaction()
            else:
                scenario = "BOT_VELOCITY"
                payload = build_bot_velocity_transaction()

            await producer.send_and_wait(settings.KAFKA_TOPIC_TRANSACTIONS, payload)

            display_amount = payload["amount"] / 100

            if scenario == "NORMAL":
                print(
                    f"📡 [NORMAL] {payload['user_id']} spent "
                    f"£{display_amount:.2f} at {payload['merchant_id']}"
                )
            elif scenario == "APP_SCAM":
                print(
                    f"🚨 [APP SCAM] Panic transfer from {payload['user_id']} "
                    f"for £{display_amount:.2f}"
                )
            elif scenario == "MULE_SWARM":
                print(
                    f"🕸️ [MULE SWARM] {payload['user_id']} paid "
                    f"{payload['merchant_id']} £{display_amount:.2f}"
                )
            else:
                print(
                    f"🤖 [BOT VELOCITY] {payload['user_id']} sent "
                    f"£{display_amount:.2f}"
                )

            await asyncio.sleep(random.uniform(0.5, 1.5))

    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(simulate_transactions())