from geoalchemy2 import Geometry
from sqlalchemy import BigInteger, Integer, Numeric, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Link(Base):
    __tablename__ = "links"

    link_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    length_miles: Mapped[float] = mapped_column(Numeric(12, 9), nullable=False)
    road_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    usdk_speed_category: Mapped[int] = mapped_column(Integer, nullable=False)
    funclass_id: Mapped[int] = mapped_column(Integer, nullable=False)
    speedcat: Mapped[int] = mapped_column(Integer, nullable=False)
    volume_value: Mapped[int] = mapped_column(Integer, nullable=False)
    volume_bin_id: Mapped[int] = mapped_column(Integer, nullable=False)
    volume_year: Mapped[int] = mapped_column(Integer, nullable=False)
    volumes_bin_description: Mapped[str] = mapped_column(Text, nullable=False)
    geometry: Mapped[object] = mapped_column(
        Geometry(geometry_type="LINESTRING", srid=4326, spatial_index=True),
        nullable=False,
    )

    speed_records = relationship("SpeedRecord", back_populates="link", cascade="all, delete-orphan")

