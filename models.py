from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String
from sqlalchemy.dialects.postgresql import JSONB

from database import Base


class TransactionRecord(Base):
    """
    The Immutable Ledger Table.
    Every transaction evaluated by the fraud engine is permanently recorded here.
    """

    __tablename__ = "transactions"

    transaction_id = Column(String, primary_key=True, index=True)

    trace_id = Column(String, index=True, nullable=False)
    user_id = Column(String, index=True, nullable=False)
    merchant_id = Column(String, index=True, nullable=False)

    # Integer minor units only.
    # Example: £10.50 = 1050
    amount = Column(Integer, nullable=False)
    currency = Column(String, nullable=False)

    status = Column(String, nullable=False)
    risk_score = Column(Integer, nullable=False)

    # Dashboard / observability fields
    risk_reasons = Column(JSONB, nullable=False, default=list)
    redis_eval_ms = Column(Float, nullable=False, default=0.0)
    sender_velocity_count = Column(Integer, nullable=True)
    receiver_unique_sender_count = Column(Integer, nullable=True)

    received_at = Column(DateTime, default=datetime.utcnow, nullable=False)
