from __future__ import annotations

from typing import Any

import pytest

from braintrust_migrate.batching import approx_events_insert_payload_bytes
from braintrust_migrate.streaming_utils import stream_btql_sorted_events


@pytest.mark.asyncio
async def test_stream_batches_using_post_rewrite_payload_size() -> None:
    events = [
        {"id": "a", "input": "x"},
        {"id": "b", "input": "y"},
    ]
    max_bytes = approx_events_insert_payload_bytes(events) - 1

    page_calls = 0
    inserted_batches: list[list[str]] = []
    inserted_total = 0
    inserted_bytes_total = 0
    attachments_copied_total = 0

    def set_inserted_total(n: int) -> None:
        nonlocal inserted_total
        inserted_total += n

    def set_inserted_bytes_total(n: int) -> None:
        nonlocal inserted_bytes_total
        inserted_bytes_total += n

    def set_attachments_copied_total(n: int) -> None:
        nonlocal attachments_copied_total
        attachments_copied_total += n

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

    await stream_btql_sorted_events(
        fetch_page=fetch_page,
        page_limit=100,
        get_last_pk=lambda: None,
        set_last_pk=lambda _pk: None,
        save_state=lambda: None,
        page_event_filter=None,
        event_to_insert=lambda event: {
            "id": event["id"],
            "input": event["input"],
        },
        seen_db=None,
        insert_batch_size=100,
        insert_max_bytes=max_bytes,
        rewrite_event_in_place=rewrite_event_in_place,
        insert_events=insert_events,
        is_http_413=lambda _exc: False,
        on_single_413=None,
        incr_fetched=lambda _n: None,
        incr_inserted=set_inserted_total,
        incr_inserted_bytes=set_inserted_bytes_total,
        incr_skipped_deleted=None,
        incr_skipped_seen=None,
        incr_attachments_copied=set_attachments_copied_total,
    )

    assert inserted_batches == [["a"], ["b"]]
    assert sum(len(batch) for batch in inserted_batches) == 2
    assert inserted_total == 2
    assert inserted_bytes_total > max_bytes
    assert attachments_copied_total == 2
