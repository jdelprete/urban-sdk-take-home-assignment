from typing import Any

from pydantic import BaseModel, Field


DAY_NAME_TO_ID = {
    "sunday": 1,
    "monday": 2,
    "tuesday": 3,
    "wednesday": 4,
    "thursday": 5,
    "friday": 6,
    "saturday": 7,
}

DAY_ID_TO_NAME = {
    1: "Sunday",
    2: "Monday",
    3: "Tuesday",
    4: "Wednesday",
    5: "Thursday",
    6: "Friday",
    7: "Saturday",
}

PERIOD_NAME_TO_ID = {
    "overnight": 1,
    "early morning": 2,
    "am peak": 3,
    "midday": 4,
    "early afternoon": 5,
    "pm peak": 6,
    "evening": 7,
}

PERIOD_ID_TO_NAME = {
    1: "Overnight",
    2: "Early Morning",
    3: "AM Peak",
    4: "Midday",
    5: "Early Afternoon",
    6: "PM Peak",
    7: "Evening",
}


def parse_day(day: str) -> int:
    normalized = day.strip().lower()
    if normalized not in DAY_NAME_TO_ID:
        valid_days = ", ".join(DAY_ID_TO_NAME.values())
        raise ValueError(f"Invalid day '{day}'. Expected one of: {valid_days}")
    return DAY_NAME_TO_ID[normalized]


def parse_period(period: str) -> int:
    normalized = period.strip().lower()
    if normalized not in PERIOD_NAME_TO_ID:
        valid_periods = ", ".join(PERIOD_ID_TO_NAME.values())
        raise ValueError(f"Invalid period '{period}'. Expected one of: {valid_periods}")
    return PERIOD_NAME_TO_ID[normalized]


class AggregateFeature(BaseModel):
    link_id: int
    road_name: str | None
    length_miles: float
    average_speed: float
    freeflow_speed: float
    geometry: dict[str, Any]


class LinkAggregateDetail(AggregateFeature):
    sample_count: int
    average_confidence: float
    min_speed: float
    max_speed: float
    p85_speed: float
    p95_speed: float
    day: str
    period: str


class SlowLinkFeature(BaseModel):
    link_id: int
    road_name: str | None
    length_miles: float
    slow_days: int
    average_speed: float
    geometry: dict[str, Any]


class SpatialFilterRequest(BaseModel):
    day: str = Field(..., examples=["Monday"])
    period: str = Field(..., examples=["AM Peak"])
    bbox: list[float] = Field(
        ...,
        min_length=4,
        max_length=4,
        description="[min_lon, min_lat, max_lon, max_lat]",
        examples=[[-81.8, 30.1, -81.6, 30.3]],
    )
