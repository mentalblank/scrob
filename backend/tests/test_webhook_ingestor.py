import time
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from webhooks import webhook_router, INGESTION_EVENT_BUFFER


@pytest.fixture
def client():
    INGESTION_EVENT_BUFFER.clear()
    app = FastAPI()
    app.include_router(webhook_router)
    return TestClient(app)


def test_plex_webhook_fast_ingest(client: TestClient):
    start = time.perf_counter()
    response = client.post("/webhooks/plex", json={"event": "media.scrobble", "ratingKey": "12345"})
    elapsed_ms = (time.perf_counter() - start) * 1000

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "accepted"
    assert data["provider"] == "plex"
    assert "trace_id" in data
    assert elapsed_ms < 50.0

    assert len(INGESTION_EVENT_BUFFER) == 1
    assert INGESTION_EVENT_BUFFER[0]["provider"] == "plex"


def test_jellyfin_webhook_fast_ingest(client: TestClient):
    response = client.post("/webhooks/jellyfin", json={"NotificationType": "ItemAdded", "ItemId": "abc"})
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "accepted"
    assert data["provider"] == "jellyfin"


def test_emby_webhook_fast_ingest(client: TestClient):
    response = client.post("/webhooks/emby", json={"Event": "playback.stop", "Item": {"Id": "789"}})
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "accepted"
    assert data["provider"] == "emby"


def test_sonarr_webhook_fast_ingest(client: TestClient):
    response = client.post("/webhooks/sonarr", json={"eventType": "Download", "series": {"id": 1}})
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "accepted"
    assert data["provider"] == "sonarr"


def test_radarr_webhook_fast_ingest(client: TestClient):
    response = client.post("/webhooks/radarr", json={"eventType": "Download", "movie": {"id": 10}})
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "accepted"
    assert data["provider"] == "radarr"
