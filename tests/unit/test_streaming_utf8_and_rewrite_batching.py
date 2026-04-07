from __future__ import annotations

from typing import Any

import pytest

from braintrust_migrate.batching import approx_json_bytes
from braintrust_migrate.streaming_utils import (
    EventsStreamState,
    stream_btql_sorted_events_buffered,
)


@pytest.mark.asyncio
async def test_stream_batches_using_post_rewrite_payload_size() -> None:
    max_bytes = approx_json_bytes({"id": "a", "input": "🙂" * 16}) + 1

    page_calls = 0
    inserted_batches: list[list[str]] = []
    insert_updates: list[dict[str, Any]] = []
    state = EventsStreamState()

    async def fetch_page(_limit: int) -> dict[str, Any]:
        nonlocal page_calls
        page_calls += 1
        if page_calls == 1:
            return {
                "events": [
                    {"id": "a", "_pagination_key": "p1", "input": "x"},
                    {"id": "b", "_pagination_key": "p2", "input": "y"},
                ],
                "btql_last_pagination_key": "p2",
            }
        return {"events": [], "btql_last_pagination_key": None}

    async def rewrite_event_in_place(event: dict[str, Any]) -> int:
        event["input"] = "🙂" * 16
        return 1

    async def insert_events(batch: list[dict[str, Any]]) -> None:
        inserted_batches.append([str(event["id"]) for event in batch])

    await stream_btql_sorted_events_buffered(
        fetch_page=fetch_page,
        page_limit=100,
        state=state,
        save_state=lambda: None,
        page_event_filter=None,
        event_to_insert=lambda event: {
            "id": event["id"],
            "input": event["input"],
        },
        seen_db=None,
        rewrite_event_in_place=rewrite_event_in_place,
        insert_events=insert_events,
        flush_max_rows=100,
        flush_max_bytes=max_bytes,
        is_http_413=lambda _exc: False,
        on_single_413=None,
        hooks={"on_insert": insert_updates.append},
    )

    assert inserted_batches == [["a", "b"]]
    assert sum(len(batch) for batch in inserted_batches) == 2
    assert state.inserted_events == 2
    assert state.inserted_bytes > max_bytes
    assert state.attachments_copied == 2
    assert len(insert_updates) == 1
    assert insert_updates[0]["flush_buffer_bytes"] > max_bytes
