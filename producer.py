import asyncio
import json
import random
import uuid
from aiokafka import AIOKafkaProducer
from config import settings

MERCHANTS = ["m-9921", "m-4432", "m-1102", "m-5599"]
CURRENCIES = ["GBP", "EUR", "USD"]


def pounds_to_minor_units(value: float) -> int:
    """
    Convert a decimal currency amount to integer minor units.
    Example: 10.50 -> 1050
    """
    return int(round(value * 100))


async def simulate_transactions():
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    await producer.start()
    print("🚀 Simulator streaming complex financial activity...")

    try:
        while True:
            is_scam_panic = random.random() < 0.10

            amount_major = (
                random.uniform(500.0, 3500.0)
                if is_scam_panic
                else random.uniform(5.0, 150.0)
            )

            payload = {
                "transaction_id": str(uuid.uuid4()),
                "user_id": f"user_{random.randint(1000, 1050)}",
                "merchant_id": random.choice(MERCHANTS),
                "amount": pounds_to_minor_units(amount_major),
                "currency": random.choice(CURRENCIES),
                "device_ip": f"192.168.1.{random.randint(1, 255)}",
                "behavioral_metadata": {
                    "time_to_complete_ms": (
                        random.randint(1200, 3000)
                        if is_scam_panic
                        else random.randint(15000, 60000)
                    ),
                    "is_new_payee": True if is_scam_panic else random.choice([True, False]),
                    "password_reset_24h": True if is_scam_panic else False,
                },
            }

            await producer.send_and_wait(settings.KAFKA_TOPIC_TRANSACTIONS, payload)

            if is_scam_panic:
                print(
                    f"🚨 [APP SCAM INJECTED] High-speed panic transfer from "
                    f"{payload['user_id']} for £{payload['amount'] / 100:.2f}!"
                )
            else:
                print(
                    f"📡 [NORMAL] {payload['user_id']} spent "
                    f"£{payload['amount'] / 100:.2f}"
                )

            await asyncio.sleep(random.uniform(0.5, 2.0))

    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(simulate_transactions())    