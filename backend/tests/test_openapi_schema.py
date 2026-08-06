import pytest
from fastapi import FastAPI
from webhooks import webhook_router
from schemas.openapi_specs import generate_openapi_spec


def test_generate_openapi_spec():
    app = FastAPI(title="Scrob API", version="2.0.0")
    app.include_router(webhook_router)

    spec = generate_openapi_spec(app)

    assert spec["openapi"].startswith("3.")
    assert spec["info"]["title"] == "Scrob Universal Sync Engine API"
    assert spec["info"]["version"] == "2.0.0"
    assert "paths" in spec

    paths = spec["paths"]
    assert "/webhooks/plex" in paths
    assert "/webhooks/jellyfin" in paths
    assert "/webhooks/emby" in paths
    assert "/webhooks/sonarr" in paths
    assert "/webhooks/radarr" in paths
