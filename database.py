from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base
from config import settings

# 1. Build the Asynchronous Engine
# This is the core connection pool that talks directly to Docker PostgreSQL
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=True,  # Set to True so it prints the raw SQL it writes to your terminal!
    future=True
)

# 2. Create the Session Factory
# Every time a transaction needs to be saved, it requests a temporary "session" from this factory
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)

# 3. Initialize the Declarative Base
# All of our database table blueprints will inherit from this base class
Base = declarative_base()

# 4. FastAPI Dependency function
# We will use this later to safely open and close connections during web requests
async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()