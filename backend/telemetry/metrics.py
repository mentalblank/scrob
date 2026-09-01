import time
from typing import Any
from fastapi import FastAPI, Response


class TelemetryRegistry:
    def __init__(self):
        self.webhook_ingest_counts: dict[str, int] = {}
        self.sync_duration_seconds: list[dict[str, Any]] = []
        self.resolver_match_counts: dict[str, int] = {}
        self.active_worker_jobs: int = 0

    def record_webhook_ingest(self, provider: str, status_code: int = 202) -> None:
        key = f"{provider}:{status_code}"
        self.webhook_ingest_counts[key] = self.webhook_ingest_counts.get(key, 0) + 1

    def record_sync_duration(self, provider: str, duration_sec: float) -> None:
        self.sync_duration_seconds.append({"provider": provider, "duration": duration_sec})

    def record_resolver_match(self, match_mode: str) -> None:
        self.resolver_match_counts[match_mode] = (
            self.resolver_match_counts.get(match_mode, 0) + 1
        )

    def set_active_worker_jobs(self, count: int) -> None:
        self.active_worker_jobs = count

    def generate_prometheus_metrics(self) -> str:
        lines = []

        # Webhook ingest counter
        lines.append("# HELP scrobble_webhook_ingest_total Total webhooks ingested")
        lines.append("# TYPE scrobble_webhook_ingest_total counter")
        for key, count in self.webhook_ingest_counts.items():
            provider, status = key.split(":")
            lines.append(
                f'scrobble_webhook_ingest_total{{provider="{provider}",status="{status}"}} {count}'
            )

        # Resolver match counter
        lines.append("# HELP scrobble_resolver_match_total Total asset resolver matches")
        lines.append("# TYPE scrobble_resolver_match_total counter")
        for mode, count in self.resolver_match_counts.items():
            lines.append(f'scrobble_resolver_match_total{{match_mode="{mode}"}} {count}')

        # Active worker jobs gauge
        lines.append("# HELP scrobble_active_worker_jobs Current active worker jobs")
        lines.append("# TYPE scrobble_active_worker_jobs gauge")
        lines.append(f"scrobble_active_worker_jobs {self.active_worker_jobs}")

        return "\n".join(lines) + "\n"


telemetry_registry = TelemetryRegistry()


def setup_telemetry(app: FastAPI) -> None:
    @app.get("/metrics")
    def metrics_endpoint():
        content = telemetry_registry.generate_prometheus_metrics()
        return Response(content=content, media_type="text/plain; version=0.0.4")
