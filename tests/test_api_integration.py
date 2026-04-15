import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.db.session import SessionLocal
from app.main import app


@pytest.fixture(scope="module")
def db_session():
    try:
        session = SessionLocal()
        session.execute(text("SELECT 1"))
        links_count = session.execute(text("SELECT COUNT(*) FROM links")).scalar_one()
        speeds_count = session.execute(text("SELECT COUNT(*) FROM speed_records")).scalar_one()
    except Exception as exc:  # pragma: no cover - skip path depends on local runtime
        pytest.skip(f"Integration database is unavailable: {exc}")

    if links_count == 0 or speeds_count == 0:
        session.close()
        pytest.skip("Integration database is empty. Run `make ingest` before integration tests.")

    try:
        yield session
    finally:
        session.close()


@pytest.fixture(scope="module")
def integration_client(db_session):
    # The fixture depends on db_session to ensure the database is reachable before requests begin.
    with TestClient(app) as client:
        yield client


@pytest.fixture(scope="module")
def sample_link_id(db_session) -> int:
    return db_session.execute(
        text(
            """
            SELECT link_id
            FROM speed_records
            WHERE day_of_week = 2 AND period_id = 3
            ORDER BY link_id
            LIMIT 1
            """
        )
    ).scalar_one()


@pytest.mark.integration
def test_healthcheck_integration(integration_client):
    response = integration_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.integration
def test_aggregates_integration_returns_real_rows(integration_client):
    response = integration_client.get("/aggregates/", params={"day": "Monday", "period": "AM Peak"})

    assert response.status_code == 200
    payload = response.json()
    assert payload
    first = payload[0]
    assert isinstance(first["link_id"], int)
    assert isinstance(first["length_miles"], float)
    assert isinstance(first["average_speed"], float)
    assert isinstance(first["freeflow_speed"], float)
    assert first["geometry"]["type"] == "LineString"


@pytest.mark.integration
def test_aggregate_detail_integration_returns_real_link(integration_client, sample_link_id):
    response = integration_client.get(
        f"/aggregates/{sample_link_id}",
        params={"day": "Monday", "period": "AM Peak"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["link_id"] == sample_link_id
    assert payload["day"] == "Monday"
    assert payload["period"] == "AM Peak"
    assert payload["sample_count"] >= 1
    assert payload["max_speed"] >= payload["min_speed"]


@pytest.mark.integration
def test_slow_links_integration_returns_threshold_matches(integration_client):
    response = integration_client.get(
        "/patterns/slow_links/",
        params={"period": "AM Peak", "threshold": 20, "min_days": 1},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload
    assert payload[0]["slow_days"] >= 1
    assert payload[0]["average_speed"] < 20


@pytest.mark.integration
def test_spatial_filter_integration_returns_bbox_matches(integration_client):
    response = integration_client.post(
        "/aggregates/spatial_filter/",
        json={"day": "Monday", "period": "AM Peak", "bbox": [-81.8, 30.1, -81.6, 30.3]},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload
    first = payload[0]
    assert -81.8 <= first["geometry"]["coordinates"][0][0] <= -81.6
    assert 30.1 <= first["geometry"]["coordinates"][0][1] <= 30.3


@pytest.mark.integration
def test_aggregate_detail_integration_returns_404_for_missing_link(integration_client):
    response = integration_client.get(
        "/aggregates/999999999999",
        params={"day": "Monday", "period": "AM Peak"},
    )

    assert response.status_code == 404


@pytest.mark.integration
def test_validation_errors_integration_are_consistent(integration_client):
    response = integration_client.get("/aggregates/", params={"day": "Funday", "period": "AM Peak"})

    assert response.status_code == 422
    assert "Invalid day 'Funday'" in response.json()["detail"]
