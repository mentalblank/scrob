import asyncio
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from models.domain import (
    DomainBase,
    Show,
    Season,
    Episode,
    ExternalID,
    User,
    WatchEvent,
)
from services.external_id_registry import ExternalIDRegistryService
from services.asset_resolver import AssetResolver
from services.media_service import MediaService
from services.history_service import HistoryService
from services.cqrs_read_models import CQRSReadModels
from events.bus import EventBus
from sync.providers.plex_sync import PlexSyncProvider
from workers import WorkerQueue
from telemetry import telemetry_registry
from scripts.migrate_legacy_data import LegacyMigrationETL


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:", echo=False)
    DomainBase.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def test_e2e_full_cutover_pipeline(db_session: Session):
    # 1. User creation
    user = User(username="cutover_user", email="cutover@scrob.io", api_key="key_cutover")
    db_session.add(user)
    db_session.commit()

    # 2. Migration ETL execution
    etl_res = asyncio.run(LegacyMigrationETL.run_migration(db_session))
    db_session.commit()
    assert etl_res["shows_migrated"] == 1

    # 3. External ID Registry & Asset Resolution
    show = db_session.query(Show).first()
    assert show is not None
    resolved_show = asyncio.run(
        AssetResolver.resolve_show(
            db_session,
            show_ref="1396",
            provider="tmdb",
            title=show.canonical_title,
        )
    )
    assert resolved_show.id == show.id
    telemetry_registry.record_resolver_match("exact_id")

    # 4. Event Bus & Webhook ingestion simulation
    bus = EventBus(redis_client=None)
    event_payload = {
        "event": "media.scrobble",
        "ratingKey": "1396",
        "grandparentTitle": show.canonical_title,
        "parentIndex": 1,
        "index": 1,
    }
    msg_id = asyncio.run(bus.publish("scrobble_events", event_payload))
    telemetry_registry.record_webhook_ingest("plex", 202)

    # 5. Modular Sync Engine Execution
    plex_sync = PlexSyncProvider()
    scrob_res = asyncio.run(plex_sync.process_scrobble(db_session, user.id, event_payload))
    assert scrob_res.success is True

    # 6. Record Watch Event & History Service
    watch_event = asyncio.run(
        HistoryService.record_watch_event(
            db_session, user_id=user.id, asset_type="episode", asset_id=1
        )
    )
    db_session.commit()

    # 7. CQRS Read Model & Progress Summary
    progress = asyncio.run(
        CQRSReadModels.get_show_progress_summary(db_session, user.id, show.id)
    )
    assert progress["total_episodes"] == 1
    assert progress["watched_episodes"] == 1
    assert progress["progress_percent"] == 100.0
    assert progress["is_completed"] is True

    # 8. Background Worker Queue
    worker_q = WorkerQueue(redis_pool=None)
    job_id = asyncio.run(worker_q.enqueue_task("run_full_sync_task", user_id=user.id, provider="plex"))
    job_status = asyncio.run(worker_q.get_job_status(job_id))
    assert job_status["status"] == "completed"

    # 9. Verify Prometheus Telemetry output
    metrics_str = telemetry_registry.generate_prometheus_metrics()
    assert 'scrobble_webhook_ingest_total{provider="plex",status="202"}' in metrics_str
    assert 'scrobble_resolver_match_total{match_mode="exact_id"}' in metrics_str
