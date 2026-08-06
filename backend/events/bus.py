import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class EventBus:
    def __init__(self, redis_client: Any = None):
        self.redis = redis_client
        self._memory_streams: dict[str, list[dict[str, Any]]] = {}
        self._dead_letter_stream: list[dict[str, Any]] = []

    async def publish(self, stream: str, payload: dict[str, Any]) -> str:
        if self.redis is not None:
            try:
                msg_id = await self.redis.xadd(stream, {"data": json.dumps(payload)})
                return str(msg_id)
            except Exception as e:
                logger.warning("Redis publish failed, falling back to memory bus: %s", e)

        # Fallback in-memory stream publishing
        if stream not in self._memory_streams:
            self._memory_streams[stream] = []
        msg_id = f"mem_{len(self._memory_streams[stream]) + 1}"
        event_record = {"id": msg_id, "stream": stream, "payload": payload, "acked": False}
        self._memory_streams[stream].append(event_record)
        return msg_id

    async def consume(
        self, stream: str, group: str, consumer_id: str, count: int = 10
    ) -> list[dict[str, Any]]:
        if self.redis is not None:
            try:
                # Ensure consumer group exists
                try:
                    await self.redis.xgroup_create(stream, group, id="0", mkstream=True)
                except Exception:
                    pass
                entries = await self.redis.xreadgroup(
                    groupname=group,
                    consumername=consumer_id,
                    streams={stream: ">"},
                    count=count,
                )
                results = []
                for s_name, msgs in entries:
                    for msg_id, fields in msgs:
                        data = json.loads(fields.get(b"data", fields.get("data", "{}")))
                        results.append({"id": str(msg_id), "payload": data})
                return results
            except Exception as e:
                logger.warning("Redis consume failed, falling back to memory bus: %s", e)

        # In-memory stream consumption
        events = self._memory_streams.get(stream, [])
        unacked = [e for e in events if not e["acked"]][:count]
        return [{"id": e["id"], "payload": e["payload"]} for e in unacked]

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        if self.redis is not None:
            try:
                await self.redis.xack(stream, group, message_id)
                return
            except Exception as e:
                logger.warning("Redis ack failed: %s", e)

        # Memory ack
        events = self._memory_streams.get(stream, [])
        for e in events:
            if e["id"] == message_id:
                e["acked"] = True
                break

    async def move_to_dead_letter(
        self, stream: str, payload: dict[str, Any], error_reason: str
    ) -> str:
        dead_payload = {
            "original_stream": stream,
            "payload": payload,
            "error_reason": error_reason,
            "retry_count_exceeded": True,
        }
        dl_stream = f"{stream}_failed"
        return await self.publish(dl_stream, dead_payload)

    def get_dead_letter_items(self, stream: str = "scrobble_events") -> list[dict[str, Any]]:
        dl_stream = f"{stream}_failed"
        return self._memory_streams.get(dl_stream, [])
