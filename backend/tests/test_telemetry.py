import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from telemetry import telemetry_registry, setup_telemetry, TelemetryRegistry


@pytest.fixture
def client():
    app = FastAPI()
    setup_telemetry(app)
    return TestClient(app)


def test_telemetry_metrics_recording():
    registry = TelemetryRegistry()

    registry.record_webhook_ingest("plex", 202)
    registry.record_webhook_ingest("plex", 202)
    registry.record_webhook_ingest("jellyfin", 202)

    registry.record_resolver_match("exact_id")
    registry.record_resolver_match("fuzzy_title")

    registry.set_active_worker_jobs(3)

    output = registry.generate_prometheus_metrics()
    assert 'scrobble_webhook_ingest_total{provider="plex",status="202"} 2' in output
    assert 'scrobble_webhook_ingest_total{provider="jellyfin",status="202"} 1' in output
    assert 'scrobble_resolver_match_total{match_mode="exact_id"} 1' in output
    assert 'scrobble_active_worker_jobs 3' in output


def test_metrics_endpoint(client: TestClient):
    telemetry_registry.record_webhook_ingest("sonarr", 202)
    response = client.get("/metrics")

    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    assert 'scrobble_webhook_ingest_total{provider="sonarr",status="202"}' in response.text
