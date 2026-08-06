import asyncio
import pytest
from events.bus import EventBus


def test_publish_and_consume_event_memory_fallback():
    bus = EventBus(redis_client=None)

    payload = {"event": "media.scrobble", "ratingKey": "12345", "user": "alice"}
    msg_id = asyncio.run(bus.publish("scrobble_events", payload))

    assert msg_id.startswith("mem_")

    consumed = asyncio.run(bus.consume("scrobble_events", group="workers", consumer_id="w1"))
    assert len(consumed) == 1
    assert consumed[0]["id"] == msg_id
    assert consumed[0]["payload"]["ratingKey"] == "12345"


def test_ack_event():
    bus = EventBus(redis_client=None)
    msg_id = asyncio.run(bus.publish("scrobble_events", {"test": "data"}))

    consumed_before = asyncio.run(bus.consume("scrobble_events", group="workers", consumer_id="w1"))
    assert len(consumed_before) == 1

    asyncio.run(bus.ack("scrobble_events", group="workers", message_id=msg_id))

    consumed_after = asyncio.run(bus.consume("scrobble_events", group="workers", consumer_id="w1"))
    assert len(consumed_after) == 0


def test_dead_letter_stream():
    bus = EventBus(redis_client=None)
    failed_payload = {"event": "corrupt_payload", "raw": "invalid_json"}

    dl_msg_id = asyncio.run(
        bus.move_to_dead_letter(
            stream="scrobble_events",
            payload=failed_payload,
            error_reason="Max retries (3) exceeded",
        )
    )

    assert dl_msg_id is not None
    dl_items = bus.get_dead_letter_items("scrobble_events")
    assert len(dl_items) == 1
    assert dl_items[0]["payload"]["error_reason"] == "Max retries (3) exceeded"
    assert dl_items[0]["payload"]["original_stream"] == "scrobble_events"
