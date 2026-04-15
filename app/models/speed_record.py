from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, ForeignKey, Index, Integer, SmallInteger
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class SpeedRecord(Base):
    __tablename__ = "speed_records"
    __table_args__ = (
        Index("ix_speed_records_link_id_timestamp_utc", "link_id", "timestamp_utc"),
        Index("ix_speed_records_day_period", "day_of_week", "period_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    link_id: Mapped[int] = mapped_column(ForeignKey("links.link_id"), nullable=False, index=True)
    timestamp_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    day_of_week: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    period_id: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    average_speed: Mapped[float] = mapped_column(Float, nullable=False)
    freeflow: Mapped[float] = mapped_column(Float, nullable=False)
    sample_count: Mapped[int] = mapped_column("sample_count", Integer, nullable=False)
    std_dev: Mapped[float] = mapped_column(Float, nullable=False)
    min_speed: Mapped[float] = mapped_column("min_speed", Float, nullable=False)
    max_speed: Mapped[float] = mapped_column("max_speed", Float, nullable=False)
    confidence: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    average_pct_85: Mapped[float] = mapped_column(Float, nullable=False)
    average_pct_95: Mapped[float] = mapped_column(Float, nullable=False)

    link = relationship("Link", back_populates="speed_records")

