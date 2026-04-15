from app.api.routes import _aggregate_statement
from app.schemas.traffic import parse_day, parse_period


def test_healthcheck(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_get_aggregates_returns_serialized_features(client, fake_session):
    response = client.get("/aggregates/", params={"day": "Monday", "period": "AM Peak"})

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 2
    assert payload[0]["link_id"] == 16981048
    assert payload[0]["road_name"] == "Philips Hwy"
    assert payload[0]["average_speed"] == 45.4
    assert payload[0]["freeflow_speed"] == 44.74
    assert payload[0]["geometry"]["type"] == "LineString"
    assert "ORDER BY links.link_id" in fake_session.last_sql


def test_get_aggregates_rejects_invalid_day(client):
    response = client.get("/aggregates/", params={"day": "Funday", "period": "AM Peak"})

    assert response.status_code == 422
    assert "Invalid day 'Funday'" in response.json()["detail"]


def test_get_aggregates_rejects_invalid_period(client):
    response = client.get("/aggregates/", params={"day": "Monday", "period": "Lunch Rush"})

    assert response.status_code == 422
    assert "Invalid period 'Lunch Rush'" in response.json()["detail"]


def test_get_aggregate_detail_returns_expected_shape(client):
    response = client.get("/aggregates/16981048", params={"day": "Monday", "period": "AM Peak"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["link_id"] == 16981048
    assert payload["sample_count"] == 48
    assert payload["average_confidence"] == 36.67
    assert payload["min_speed"] == 6.83
    assert payload["max_speed"] == 62.14
    assert payload["p85_speed"] == 55.72
    assert payload["p95_speed"] == 58.0
    assert payload["day"] == "Monday"
    assert payload["period"] == "AM Peak"


def test_get_aggregate_detail_returns_404_when_no_match(client, fake_session):
    fake_session.detail_row = None

    response = client.get("/aggregates/999999", params={"day": "Monday", "period": "AM Peak"})

    assert response.status_code == 404
    assert response.json()["detail"] == "No data found for link_id=999999"


def test_get_slow_links_returns_ranked_features(client, fake_session):
    response = client.get(
        "/patterns/slow_links/",
        params={"period": "AM Peak", "threshold": 20, "min_days": 1},
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["link_id"] == 23055328
    assert payload[0]["slow_days"] == 1
    assert payload[0]["average_speed"] == 0.62
    assert "HAVING count(DISTINCT anon_1.day_of_week) >= :count_1" in fake_session.last_sql


def test_get_slow_links_requires_positive_threshold(client):
    response = client.get(
        "/patterns/slow_links/",
        params={"period": "AM Peak", "threshold": 0, "min_days": 1},
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["query", "threshold"]


def test_get_slow_links_requires_min_days_at_least_one(client):
    response = client.get(
        "/patterns/slow_links/",
        params={"period": "AM Peak", "threshold": 20, "min_days": 0},
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["query", "min_days"]


def test_spatial_filter_returns_matching_features(client, fake_session):
    response = client.post(
        "/aggregates/spatial_filter/",
        json={"day": "Monday", "period": "AM Peak", "bbox": [-81.8, 30.1, -81.6, 30.3]},
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["link_id"] == 16981048
    assert "ST_MakeEnvelope" in fake_session.last_sql
    assert "ST_Intersects" in fake_session.last_sql


def test_spatial_filter_rejects_short_bbox(client):
    response = client.post(
        "/aggregates/spatial_filter/",
        json={"day": "Monday", "period": "AM Peak", "bbox": [-81.8, 30.1, -81.6]},
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "bbox"]


def test_spatial_filter_rejects_invalid_period(client):
    response = client.post(
        "/aggregates/spatial_filter/",
        json={"day": "Monday", "period": "Rush Hour", "bbox": [-81.8, 30.1, -81.6, 30.3]},
    )

    assert response.status_code == 422
    assert "Invalid period 'Rush Hour'" in response.json()["detail"]


def test_parse_day_is_case_insensitive_and_trimmed():
    assert parse_day("  monday ") == 2


def test_parse_period_is_case_insensitive_and_trimmed():
    assert parse_period(" am peak ") == 3


def test_aggregate_statement_filters_day_and_period():
    sql = str(_aggregate_statement(2, 3))

    assert "speed_records.day_of_week = :day_of_week_1" in sql
    assert "speed_records.period_id = :period_id_1" in sql
    assert "ST_AsGeoJSON(links.geometry)" in sql
