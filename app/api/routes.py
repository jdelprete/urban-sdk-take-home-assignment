import json

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import Float, cast, distinct, func, select
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models import Link, SpeedRecord
from app.schemas.traffic import (
    DAY_ID_TO_NAME,
    PERIOD_ID_TO_NAME,
    AggregateFeature,
    LinkAggregateDetail,
    SlowLinkFeature,
    SpatialFilterRequest,
    parse_day,
    parse_period,
)

router = APIRouter()


def _aggregate_statement(day_id: int, period_id: int):
    return (
        select(
            Link.link_id.label("link_id"),
            Link.road_name.label("road_name"),
            cast(Link.length_miles, Float).label("length_miles"),
            func.avg(SpeedRecord.average_speed).label("average_speed"),
            func.avg(SpeedRecord.freeflow).label("freeflow_speed"),
            func.ST_AsGeoJSON(Link.geometry).label("geometry"),
        )
        .join(SpeedRecord, SpeedRecord.link_id == Link.link_id)
        .where(SpeedRecord.day_of_week == day_id, SpeedRecord.period_id == period_id)
        .group_by(Link.link_id, Link.road_name, Link.length_miles, Link.geometry)
    )


def _build_feature(row) -> AggregateFeature:
    return AggregateFeature(
        link_id=row.link_id,
        road_name=row.road_name,
        length_miles=float(row.length_miles),
        average_speed=round(float(row.average_speed), 2),
        freeflow_speed=round(float(row.freeflow_speed), 2),
        geometry=json.loads(row.geometry),
    )


@router.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/aggregates/", response_model=list[AggregateFeature])
def get_aggregates(
    day: str = Query(..., description="Weekday name, e.g. Monday"),
    period: str = Query(..., description="Named period, e.g. AM Peak"),
    db: Session = Depends(get_db),
) -> list[AggregateFeature]:
    try:
        day_id = parse_day(day)
        period_id = parse_period(period)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    rows = db.execute(_aggregate_statement(day_id, period_id).order_by(Link.link_id)).all()
    return [_build_feature(row) for row in rows]


@router.get("/aggregates/{link_id}", response_model=LinkAggregateDetail)
def get_link_aggregate(
    link_id: int,
    day: str = Query(..., description="Weekday name, e.g. Monday"),
    period: str = Query(..., description="Named period, e.g. AM Peak"),
    db: Session = Depends(get_db),
) -> LinkAggregateDetail:
    try:
        day_id = parse_day(day)
        period_id = parse_period(period)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    statement = (
        select(
            Link.link_id.label("link_id"),
            Link.road_name.label("road_name"),
            cast(Link.length_miles, Float).label("length_miles"),
            func.avg(SpeedRecord.average_speed).label("average_speed"),
            func.avg(SpeedRecord.freeflow).label("freeflow_speed"),
            func.sum(SpeedRecord.sample_count).label("sample_count"),
            func.avg(SpeedRecord.confidence).label("average_confidence"),
            func.min(SpeedRecord.min_speed).label("min_speed"),
            func.max(SpeedRecord.max_speed).label("max_speed"),
            func.avg(SpeedRecord.average_pct_85).label("p85_speed"),
            func.avg(SpeedRecord.average_pct_95).label("p95_speed"),
            func.ST_AsGeoJSON(Link.geometry).label("geometry"),
        )
        .join(SpeedRecord, SpeedRecord.link_id == Link.link_id)
        .where(
            Link.link_id == link_id,
            SpeedRecord.day_of_week == day_id,
            SpeedRecord.period_id == period_id,
        )
        .group_by(Link.link_id, Link.road_name, Link.length_miles, Link.geometry)
    )

    row = db.execute(statement).one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail=f"No data found for link_id={link_id}")

    return LinkAggregateDetail(
        link_id=row.link_id,
        road_name=row.road_name,
        length_miles=float(row.length_miles),
        average_speed=round(float(row.average_speed), 2),
        freeflow_speed=round(float(row.freeflow_speed), 2),
        sample_count=int(row.sample_count),
        average_confidence=round(float(row.average_confidence), 2),
        min_speed=round(float(row.min_speed), 2),
        max_speed=round(float(row.max_speed), 2),
        p85_speed=round(float(row.p85_speed), 2),
        p95_speed=round(float(row.p95_speed), 2),
        day=DAY_ID_TO_NAME[day_id],
        period=PERIOD_ID_TO_NAME[period_id],
        geometry=json.loads(row.geometry),
    )


@router.get("/patterns/slow_links/", response_model=list[SlowLinkFeature])
def get_slow_links(
    period: str = Query(..., description="Named period, e.g. AM Peak"),
    threshold: float = Query(..., gt=0),
    min_days: int = Query(..., ge=1),
    db: Session = Depends(get_db),
) -> list[SlowLinkFeature]:
    try:
        period_id = parse_period(period)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    daily_speeds = (
        select(
            SpeedRecord.link_id.label("link_id"),
            SpeedRecord.day_of_week.label("day_of_week"),
            func.avg(SpeedRecord.average_speed).label("daily_average_speed"),
        )
        .where(SpeedRecord.period_id == period_id)
        .group_by(SpeedRecord.link_id, SpeedRecord.day_of_week)
        .subquery()
    )

    statement = (
        select(
            Link.link_id.label("link_id"),
            Link.road_name.label("road_name"),
            cast(Link.length_miles, Float).label("length_miles"),
            func.count(distinct(daily_speeds.c.day_of_week)).label("slow_days"),
            func.avg(daily_speeds.c.daily_average_speed).label("average_speed"),
            func.ST_AsGeoJSON(Link.geometry).label("geometry"),
        )
        .join(daily_speeds, daily_speeds.c.link_id == Link.link_id)
        .where(daily_speeds.c.daily_average_speed < threshold)
        .group_by(Link.link_id, Link.road_name, Link.length_miles, Link.geometry)
        .having(func.count(distinct(daily_speeds.c.day_of_week)) >= min_days)
        .order_by(func.avg(daily_speeds.c.daily_average_speed).asc())
    )

    rows = db.execute(statement).all()
    return [
        SlowLinkFeature(
            link_id=row.link_id,
            road_name=row.road_name,
            length_miles=float(row.length_miles),
            slow_days=int(row.slow_days),
            average_speed=round(float(row.average_speed), 2),
            geometry=json.loads(row.geometry),
        )
        for row in rows
    ]


@router.post("/aggregates/spatial_filter/", response_model=list[AggregateFeature])
def get_spatial_filter_aggregates(
    payload: SpatialFilterRequest,
    db: Session = Depends(get_db),
) -> list[AggregateFeature]:
    try:
        day_id = parse_day(payload.day)
        period_id = parse_period(payload.period)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    min_lon, min_lat, max_lon, max_lat = payload.bbox
    bbox_geom = func.ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, 4326)
    statement = (
        _aggregate_statement(day_id, period_id)
        .where(func.ST_Intersects(Link.geometry, bbox_geom))
        .order_by(Link.link_id)
    )
    rows = db.execute(statement).all()
    return [_build_feature(row) for row in rows]

