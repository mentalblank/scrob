import time
import uuid
from typing import Any
from fastapi import APIRouter, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/webhooks", tags=["webhooks"])

# Memory buffer fallback if Redis is disconnected during ingestion
INGESTION_EVENT_BUFFER: list[dict[str, Any]] = []


async def publish_webhook_event(provider: str, payload: dict[str, Any], trace_id: str) -> bool:
    event_item = {
        "trace_id": trace_id,
        "provider": provider,
        "payload": payload,
        "timestamp": time.time(),
    }
    # Append to memory buffer
    INGESTION_EVENT_BUFFER.append(event_item)
    return True


def validate_secret_token(token: str | None, expected_token: str | None = None) -> bool:
    if expected_token and token != expected_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid webhook token or signature",
        )
    return True


@router.post("/plex")
async def plex_webhook_ingest(
    request: Request,
    token: str | None = Query(None),
    x_plex_token: str | None = Header(None),
) -> Response:
    start_time = time.perf_counter()
    auth_token = token or x_plex_token
    validate_secret_token(auth_token)

    try:
        payload = await request.json()
    except Exception:
        # Handle multipart/form-data payload from Plex
        form = await request.form()
        payload = dict(form)

    trace_id = f"plex_{uuid.uuid4().hex[:12]}"
    await publish_webhook_event("plex", payload, trace_id)

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "status": "accepted",
            "trace_id": trace_id,
            "provider": "plex",
            "ingest_latency_ms": round(elapsed_ms, 3),
        },
    )


@router.post("/jellyfin")
async def jellyfin_webhook_ingest(
    request: Request,
    token: str | None = Query(None),
) -> Response:
    start_time = time.perf_counter()
    validate_secret_token(token)

    payload = await request.json()
    trace_id = f"jellyfin_{uuid.uuid4().hex[:12]}"
    await publish_webhook_event("jellyfin", payload, trace_id)

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "status": "accepted",
            "trace_id": trace_id,
            "provider": "jellyfin",
            "ingest_latency_ms": round(elapsed_ms, 3),
        },
    )


@router.post("/emby")
async def emby_webhook_ingest(
    request: Request,
    token: str | None = Query(None),
) -> Response:
    start_time = time.perf_counter()
    validate_secret_token(token)

    payload = await request.json()
    trace_id = f"emby_{uuid.uuid4().hex[:12]}"
    await publish_webhook_event("emby", payload, trace_id)

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "status": "accepted",
            "trace_id": trace_id,
            "provider": "emby",
            "ingest_latency_ms": round(elapsed_ms, 3),
        },
    )


@router.post("/sonarr")
async def sonarr_webhook_ingest(
    request: Request,
    token: str | None = Query(None),
) -> Response:
    start_time = time.perf_counter()
    validate_secret_token(token)

    payload = await request.json()
    trace_id = f"sonarr_{uuid.uuid4().hex[:12]}"
    await publish_webhook_event("sonarr", payload, trace_id)

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "status": "accepted",
            "trace_id": trace_id,
            "provider": "sonarr",
            "ingest_latency_ms": round(elapsed_ms, 3),
        },
    )


@router.post("/radarr")
async def radarr_webhook_ingest(
    request: Request,
    token: str | None = Query(None),
) -> Response:
    start_time = time.perf_counter()
    validate_secret_token(token)

    payload = await request.json()
    trace_id = f"radarr_{uuid.uuid4().hex[:12]}"
    await publish_webhook_event("radarr", payload, trace_id)

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "status": "accepted",
            "trace_id": trace_id,
            "provider": "radarr",
            "ingest_latency_ms": round(elapsed_ms, 3),
        },
    )
