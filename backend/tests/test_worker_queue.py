import asyncio
import pytest
from workers import WorkerQueue


def test_enqueue_full_sync_task():
    queue = WorkerQueue(redis_pool=None)
    job_id = asyncio.run(queue.enqueue_task("run_full_sync_task", user_id=1, provider="plex"))

    assert job_id.startswith("job_")
    status_info = asyncio.run(queue.get_job_status(job_id))
    assert status_info["status"] == "completed"
    assert status_info["progress"] == 100
    assert status_info["result"]["user_id"] == 1
    assert status_info["result"]["items_synced"] == 42


def test_enqueue_metadata_enrichment_task():
    queue = WorkerQueue(redis_pool=None)
    job_id = asyncio.run(queue.enqueue_task("run_metadata_enrichment_task", show_id=10))

    status_info = asyncio.run(queue.get_job_status(job_id))
    assert status_info["status"] == "completed"
    assert status_info["result"]["show_id"] == 10


def test_enqueue_image_fetch_task():
    queue = WorkerQueue(redis_pool=None)
    job_id = asyncio.run(
        queue.enqueue_task(
            "run_image_fetch_task",
            image_url="https://image.tmdb.org/poster.jpg",
            asset_type="poster",
        )
    )

    status_info = asyncio.run(queue.get_job_status(job_id))
    assert status_info["status"] == "completed"
    assert status_info["result"]["cached_path"] == "/cache/images/optimized.webp"


def test_unknown_task_rejection():
    queue = WorkerQueue(redis_pool=None)
    with pytest.raises(ValueError, match="is not registered"):
        asyncio.run(queue.enqueue_task("non_existent_task"))
