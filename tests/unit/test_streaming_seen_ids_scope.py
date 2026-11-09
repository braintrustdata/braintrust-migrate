from __future__ import annotations

from typing import Any

import pytest

from braintrust_migrate.streaming_utils import (
    EventsStreamState,
    SeenIdsDB,
    stream_btql_sorted_events_buffered,
)

# Two datasets fetched as one group. Copies and versions of a dataset keep
# their row ids, so both datasets hold a row with id "shared".
GROUP_ROWS = [
    {"id": "shared", "dataset_id": "dataset-a", "_pagination_key": "p1"},
    {"id": "only-a", "dataset_id": "dataset-a", "_pagination_key": "p2"},
    {"id": "shared", "dataset_id": "dataset-b", "_pagination_key": "p3"},
]


async def _stream_group(
    seen_db: SeenIdsDB, rows: list[dict[str, Any]], *, flush_max_rows: int = 100
) -> tuple[list[tuple[str, str]], EventsStreamState]:
    pages = [rows, []]
    inserted: list[tuple[str, str]] = []
    state = EventsStreamState()

    async def fetch_page(_limit: int) -> dict[str, Any]:
        events = pages.pop(0)
        last_key = events[-1]["_pagination_key"] if events else None
        return {"events": events, "btql_last_pagination_key": last_key}

    async def insert_events(batch: list[dict[str, Any]]) -> None:
        inserted.extend((event["dataset_id"], event["id"]) for event in batch)

    await stream_btql_sorted_events_buffered(
        fetch_page=fetch_page,
        page_limit=100,
        state=state,
        save_state=lambda: None,
        page_event_filter=None,
        event_to_insert=lambda event: {
            "id": event["id"],
            "dataset_id": event["dataset_id"],
        },
        seen_db=seen_db,
        rewrite_event_in_place=None,
        insert_events=insert_events,
        flush_max_rows=flush_max_rows,
        flush_max_bytes=10_000_000,
        is_http_413=lambda _exc: False,
        on_single_413=None,
    )
    return inserted, state


@pytest.mark.asyncio
async def test_rows_sharing_an_id_across_datasets_are_all_inserted(tmp_path) -> None:
    seen_db = SeenIdsDB(str(tmp_path / "seen.sqlite3"))
    try:
        inserted, state = await _stream_group(seen_db, GROUP_ROWS)
    finally:
        seen_db.close()

    assert inserted == [
        ("dataset-a", "shared"),
        ("dataset-a", "only-a"),
        ("dataset-b", "shared"),
    ]
    assert state.skipped_seen == 0


@pytest.mark.asyncio
async def test_shared_id_survives_a_flush_between_its_two_rows(tmp_path) -> None:
    seen_db = SeenIdsDB(str(tmp_path / "seen.sqlite3"))
    try:
        first, _ = await _stream_group(seen_db, GROUP_ROWS[:2])
        second, state = await _stream_group(seen_db, GROUP_ROWS[2:])
    finally:
        seen_db.close()

    assert first == [("dataset-a", "shared"), ("dataset-a", "only-a")]
    assert second == [("dataset-b", "shared")]
    assert state.skipped_seen == 0


@pytest.mark.asyncio
async def test_a_row_already_inserted_is_still_skipped_on_resume(tmp_path) -> None:
    seen_db = SeenIdsDB(str(tmp_path / "seen.sqlite3"))
    try:
        await _stream_group(seen_db, GROUP_ROWS)
        inserted, state = await _stream_group(seen_db, GROUP_ROWS)
    finally:
        seen_db.close()

    assert inserted == []
    assert state.skipped_seen == len(GROUP_ROWS)
