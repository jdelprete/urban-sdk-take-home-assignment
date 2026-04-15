import argparse
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
from shapely.geometry import LineString, shape
from sqlalchemy import bindparam, func, insert, text
from sqlalchemy.orm import Session

from app.db.base import Base
from app.db.session import engine
from app.models import Link, SpeedRecord


def normalize_linestring(geojson_text: str) -> str:
    geometry = shape(json.loads(geojson_text))
    if geometry.geom_type == "MultiLineString":
        if len(geometry.geoms) != 1:
            raise ValueError("Expected single-part MultiLineString geometry.")
        geometry = LineString(geometry.geoms[0].coords)
    if geometry.geom_type != "LineString":
        raise ValueError(f"Unsupported geometry type: {geometry.geom_type}")
    return geometry.wkt


def parse_timestamp(timestamp_text: str) -> datetime:
    return datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))


def bootstrap_database() -> None:
    with engine.begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
    Base.metadata.create_all(bind=engine)


def ingest_links(session: Session, path: Path, batch_size: int = 5000) -> int:
    dataframe = pd.read_parquet(path)
    total = 0

    session.execute(text("TRUNCATE TABLE speed_records RESTART IDENTITY CASCADE"))
    session.execute(text("TRUNCATE TABLE links CASCADE"))
    session.commit()

    statement = insert(Link).values(
        link_id=bindparam("link_id"),
        length_miles=bindparam("length_miles"),
        road_name=bindparam("road_name"),
        usdk_speed_category=bindparam("usdk_speed_category"),
        funclass_id=bindparam("funclass_id"),
        speedcat=bindparam("speedcat"),
        volume_value=bindparam("volume_value"),
        volume_bin_id=bindparam("volume_bin_id"),
        volume_year=bindparam("volume_year"),
        volumes_bin_description=bindparam("volumes_bin_description"),
        geometry=func.ST_GeomFromText(bindparam("geometry_wkt"), 4326),
    )

    for start in range(0, len(dataframe), batch_size):
        chunk = dataframe.iloc[start : start + batch_size]
        records = []
        for row in chunk.to_dict(orient="records"):
            raw_length = row["_length"]
            length_value = float(raw_length) if isinstance(raw_length, Decimal) else float(raw_length)
            records.append(
                {
                    "link_id": int(row["link_id"]),
                    "length_miles": length_value,
                    "road_name": row["road_name"],
                    "usdk_speed_category": int(row["usdk_speed_category"]),
                    "funclass_id": int(row["funclass_id"]),
                    "speedcat": int(row["speedcat"]),
                    "volume_value": int(row["volume_value"]),
                    "volume_bin_id": int(row["volume_bin_id"]),
                    "volume_year": int(row["volume_year"]),
                    "volumes_bin_description": row["volumes_bin_description"],
                    "geometry_wkt": normalize_linestring(row["geo_json"]),
                }
            )

        session.execute(statement, records)
        session.commit()
        total += len(records)
    return total


def ingest_speed_records(session: Session, path: Path, batch_size: int = 20000) -> int:
    dataframe = pd.read_parquet(path)
    total = 0

    for start in range(0, len(dataframe), batch_size):
        chunk = dataframe.iloc[start : start + batch_size]
        records = []
        for row in chunk.to_dict(orient="records"):
            records.append(
                {
                    "link_id": int(row["link_id"]),
                    "timestamp_utc": parse_timestamp(row["date_time"]),
                    "day_of_week": int(row["day_of_week"]),
                    "period_id": int(row["period"]),
                    "average_speed": float(row["average_speed"]),
                    "freeflow": float(row["freeflow"]),
                    "sample_count": int(row["count"]),
                    "std_dev": float(row["std_dev"]),
                    "min_speed": float(row["min"]),
                    "max_speed": float(row["max"]),
                    "confidence": int(row["confidence"]),
                    "average_pct_85": float(row["average_pct_85"]),
                    "average_pct_95": float(row["average_pct_95"]),
                }
            )
        session.bulk_insert_mappings(SpeedRecord, records)
        session.commit()
        total += len(records)
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Urban SDK parquet data into PostgreSQL/PostGIS.")
    parser.add_argument("--links-path", type=Path, default=Path("link_info.parquet.gz"))
    parser.add_argument("--speeds-path", type=Path, default=Path("duval_jan1_2024.parquet.gz"))
    args = parser.parse_args()

    bootstrap_database()
    with Session(engine) as session:
        link_count = ingest_links(session, args.links_path)
        speed_count = ingest_speed_records(session, args.speeds_path)

    print(f"Ingested {link_count} links and {speed_count} speed records.")


if __name__ == "__main__":
    main()
