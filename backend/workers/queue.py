import uuid
from typing import Any, Callable
from workers.tasks import (
    run_full_sync_task,
    run_metadata_enrichment_task,
    run_image_fetch_task,
)

TASK_REGISTRY: dict[str, Callable] = {
    "run_full_sync_task": run_full_sync_task,
    "run_metadata_enrichment_task": run_metadata_enrichment_task,
    "run_image_fetch_task": run_image_fetch_task,
}


class WorkerQueue:
    def __init__(self, redis_pool: Any = None):
        self.redis_pool = redis_pool
        self._jobs: dict[str, dict[str, Any]] = {}

    async def enqueue_task(self, function_name: str, *args: Any, **kwargs: Any) -> str:
        job_id = f"job_{uuid.uuid4().hex[:12]}"
        task_func = TASK_REGISTRY.get(function_name)

        if not task_func:
            raise ValueError(f"Task function '{function_name}' is not registered.")

        # Store job status metadata
        job_info = {
            "job_id": job_id,
            "function": function_name,
            "status": "queued",
            "progress": 0,
            "result": None,
            "error": None,
        }
        self._jobs[job_id] = job_info

        # Execute task synchronously or asynchronously in fallback queue
        try:
            job_info["status"] = "in_progress"
            job_info["progress"] = 50
            result = await task_func({}, *args, **kwargs)
            job_info["status"] = "completed"
            job_info["progress"] = 100
            job_info["result"] = result
        except Exception as e:
            job_info["status"] = "failed"
            job_info["error"] = str(e)

        return job_id

    async def get_job_status(self, job_id: str) -> dict[str, Any]:
        if job_id not in self._jobs:
            return {"job_id": job_id, "status": "not_found"}
        return self._jobs[job_id]
