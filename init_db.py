import asyncio

from sqlalchemy.exc import OperationalError

from database import engine, Base
import models  # Required so SQLAlchemy registers the TransactionRecord table


async def create_database_tables() -> None:
    """
    Build the PostgreSQL schema.

    Docker can report the container as 'started' before Postgres is actually
    ready to accept TCP connections. This retry loop makes local startup
    deterministic and prevents asyncpg startup race failures.
    """

    print("🏗️ Connecting to PostgreSQL to build the vault...")

    max_attempts = 30
    delay_seconds = 2

    for attempt in range(1, max_attempts + 1):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

            print("✅ Vault built successfully! The 'transactions' table is ready.")
            return

        except (ConnectionError, OperationalError, OSError) as exc:
            print(
                f"⏳ PostgreSQL not ready yet "
                f"(attempt {attempt}/{max_attempts}): {type(exc).__name__}"
            )

            if attempt == max_attempts:
                print("❌ PostgreSQL did not become ready in time.")
                raise

            await asyncio.sleep(delay_seconds)

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(create_database_tables())