from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ⚙️ Project Metadata
    PROJECT_NAME: str = "Real-Time Fraud Engine"

    # 🐘 PostgreSQL Configuration
    POSTGRES_USER: str = "revolut_user"
    POSTGRES_PASSWORD: str = "super_secret_password"
    POSTGRES_HOST: str = "127.0.0.1"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "fraud_engine"

    # 🧠 Redis Configuration
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379

    # 🖨️ Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_TRANSACTIONS: str = "payment_transactions"

    @property
    def DATABASE_URL(self) -> str:
        """Dynamically build the async database connection string."""
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:"
            f"{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:"
            f"{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def REDIS_URL(self) -> str:
        """Dynamically build the Redis connection string."""
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/0"


settings = Settings()
