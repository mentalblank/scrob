from typing import Any
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi


def generate_openapi_spec(app: FastAPI) -> dict[str, Any]:
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title="Scrob Universal Sync Engine API",
        version="2.0.0",
        description="Provider-neutral media scrobbling, metadata resolution, and library sync engine.",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema
