import json
from collections.abc import Iterator
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.db.session import get_db
from app.main import app


class FakeResult:
    def __init__(self, rows=None, one=None):
        self._rows = rows or []
        self._one = one

    def all(self):
        return self._rows

    def one_or_none(self):
        return self._one


class FakeSession:
    def __init__(self):
        self.last_sql = None
        self.aggregate_rows = [
            SimpleNamespace(
                link_id=16981048,
                road_name="Philips Hwy",
                length_miles=0.009320565,
                average_speed=45.404,
                freeflow_speed=44.739,
                geometry=json.dumps(
                    {
                        "type": "LineString",
                        "coordinates": [[-81.59791, 30.24124], [-81.59801, 30.24135]],
                    }
                ),
            ),
            SimpleNamespace(
                link_id=23055328,
                road_name="Brady Pl Blvd",
                length_miles=0.101904844,
                average_speed=12.114,
                freeflow_speed=21.0,
                geometry=json.dumps(
                    {
                        "type": "LineString",
                        "coordinates": [[-81.6434, 30.14075], [-81.64355, 30.14159]],
                    }
                ),
            ),
        ]
        self.detail_row = SimpleNamespace(
            link_id=16981048,
            road_name="Philips Hwy",
            length_miles=0.009320565,
            average_speed=45.404,
            freeflow_speed=44.739,
            sample_count=48,
            average_confidence=36.666,
            min_speed=6.83,
            max_speed=62.14,
            p85_speed=55.719,
            p95_speed=58.004,
            geometry=json.dumps(
                {
                    "type": "LineString",
                    "coordinates": [[-81.59791, 30.24124], [-81.59801, 30.24135]],
                }
            ),
        )
        self.slow_rows = [
            SimpleNamespace(
                link_id=23055328,
                road_name="Brady Pl Blvd",
                length_miles=0.101904844,
                slow_days=1,
                average_speed=0.621,
                geometry=json.dumps(
                    {
                        "type": "LineString",
                        "coordinates": [[-81.6434, 30.14075], [-81.64355, 30.14159]],
                    }
                ),
            )
        ]

    def execute(self, statement):
        sql = str(statement)
        self.last_sql = sql

        if "sum(speed_records.sample_count)" in sql:
            if "links.link_id = :link_id_1" in sql:
                return FakeResult(one=self.detail_row)
            return FakeResult(one=None)

        if "HAVING count(DISTINCT anon_1.day_of_week)" in sql:
            return FakeResult(rows=self.slow_rows)

        if "ST_MakeEnvelope" in sql:
            return FakeResult(rows=self.aggregate_rows[:1])

        if "GROUP BY links.link_id" in sql:
            return FakeResult(rows=self.aggregate_rows)

        return FakeResult(rows=[])


@pytest.fixture
def fake_session() -> FakeSession:
    return FakeSession()


@pytest.fixture
def client(fake_session: FakeSession) -> Iterator[TestClient]:
    def override_get_db():
        yield fake_session

    app.dependency_overrides[get_db] = override_get_db
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()
