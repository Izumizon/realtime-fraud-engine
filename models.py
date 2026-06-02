from sqlalchemy import Column, String, DateTime, Integer
from datetime import datetime
from database import Base


class TransactionRecord(Base):
    """
    The Immutable Ledger Table.
    Every transaction evaluated by the fraud engine is permanently recorded here.
    """
    __tablename__ = "transactions"

    transaction_id = Column(String, primary_key=True, index=True)

    user_id = Column(String, index=True, nullable=False)
    merchant_id = Column(String, index=True, nullable=False)

    # Integer minor units only.
    # Example: £10.50 = 1050
    amount = Column(Integer, nullable=False)
    currency = Column(String, nullable=False)

    status = Column(String, nullable=False)
    risk_score = Column(Integer, nullable=False)

    received_at = Column(DateTime, default=datetime.utcnow, nullable=False)