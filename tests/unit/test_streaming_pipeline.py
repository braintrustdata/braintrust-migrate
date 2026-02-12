"""Unit tests for pipelined event streaming (pipeline=True)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from braintrust_migrate.streaming_utils import stream_btql_sorted_events


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pages(
    pages: list[list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Convert a list of event-lists into BTQL-shaped page responses.

    Returns (page_responses, counters_dict).
    """
    responses: list[dict[str, Any]] = []
    for i, events in enumerate(pages):
        last_pk = events[-1]["_pagination_key"] if events else None
        responses.append(
            {
                "events": events,
                "btql_last_pagination_key": last_pk,
            }
        )
    # Append an empty terminal page.
    responses.append({"events": [], "btql_last_pagination_key": None})
    return responses, {}


def _build_streaming_harness(
    pages: list[list[dict[str, Any]]],
    *,
    pipeline: bool = False,
    fail_on_insert_page: int | None = None,
):
    """Build all the callbacks needed by stream_btql_sorted_events.

    Returns (run_coro, state) where state has fetch_order, insert_order,
    and the final pagination key.
    """
    page_responses, _ = _make_pages(pages)
    page_idx = {"i": 0}
    pk = {"value": None}

    fetch_order: list[int] = []
    insert_calls: list[list[dict[str, Any]]] = []
    fetch_count = {"n": 0}
    insert_count = {"n": 0}
    insert_bytes = {"n": 0}

    async def fetch_page(limit: int) -> dict[str, Any]:
        idx = page_idx["i"]
        page_idx["i"] += 1
        fetch_order.append(idx)
        # Small delay to allow concurrency
        await asyncio.sleep(0.005)
        return page_responses[idx]

    async def insert_events(batch: list[dict[str, Any]]) -> None:
        page_num = len(insert_calls)
        if fail_on_insert_page is not None and page_num == fail_on_insert_page:
            raise RuntimeError("insert failed")
        insert_calls.append(batch)
        await asyncio.sleep(0.005)

    state = {
        "fetch_order": fetch_order,
        "insert_calls": insert_calls,
        "pk": pk,
        "fetch_count": fetch_count,
        "insert_count": insert_count,
        "insert_bytes": insert_bytes,
    }

    async def run() -> None:
        await stream_btql_sorted_events(
            fetch_page=fetch_page,
            page_limit=100,
            get_last_pk=lambda: pk["value"],
            set_last_pk=lambda v: pk.__setitem__("value", v),
            save_state=lambda: None,
            page_event_filter=None,
            event_to_insert=lambda e: e,
            seen_db=None,
            insert_batch_size=1000,
            insert_max_bytes=None,
            rewrite_event_in_place=None,
            insert_events=insert_events,
            is_http_413=lambda _: False,
            on_single_413=None,
            incr_fetched=lambda n: fetch_count.__setitem__("n", fetch_count["n"] + n),
            incr_inserted=lambda n: insert_count.__setitem__("n", insert_count["n"] + n),
            incr_inserted_bytes=lambda n: insert_bytes.__setitem__("n", insert_bytes["n"] + n),
            incr_skipped_deleted=None,
            incr_skipped_seen=None,
            incr_attachments_copied=None,
            pipeline=pipeline,
        )

    return run, state


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestStreamingPipeline:

    async def test_pipeline_false_is_sequential(self):
        """With pipeline=False, fetch and insert should not overlap."""
        pages = [
            [{"id": "a", "_pagination_key": "pk1"}],
            [{"id": "b", "_pagination_key": "pk2"}],
        ]
        run, state = _build_streaming_harness(pages, pipeline=False)
        await run()

        assert state["fetch_count"]["n"] == 2
        assert state["insert_count"]["n"] == 2
        assert state["pk"]["value"] == "pk2"

    async def test_pipeline_true_processes_all_events(self):
        """With pipeline=True, all events should still be processed correctly."""
        pages = [
            [{"id": "a", "_pagination_key": "pk1"}],
            [{"id": "b", "_pagination_key": "pk2"}],
            [{"id": "c", "_pagination_key": "pk3"}],
        ]
        run, state = _build_streaming_harness(pages, pipeline=True)
        await run()

        assert state["fetch_count"]["n"] == 3
        assert state["insert_count"]["n"] == 3
        assert state["pk"]["value"] == "pk3"

        # All events should have been inserted
        all_inserted_ids = [e["id"] for batch in state["insert_calls"] for e in batch]
        assert all_inserted_ids == ["a", "b", "c"]

    async def test_pipeline_commits_pk_after_insert(self):
        """Pagination key should be committed (save_state) after inserts."""
        save_state_pks: list[str | None] = []
        pk = {"value": None}

        pages = [
            [{"id": "a", "_pagination_key": "pk1"}],
            [{"id": "b", "_pagination_key": "pk2"}],
        ]
        page_responses, _ = _make_pages(pages)
        page_idx = {"i": 0}

        async def fetch_page(limit: int) -> dict[str, Any]:
            idx = page_idx["i"]
            page_idx["i"] += 1
            return page_responses[idx]

        async def insert_events(batch: list[dict[str, Any]]) -> None:
            pass

        def save_state() -> None:
            save_state_pks.append(pk["value"])

        await stream_btql_sorted_events(
            fetch_page=fetch_page,
            page_limit=100,
            get_last_pk=lambda: pk["value"],
            set_last_pk=lambda v: pk.__setitem__("value", v),
            save_state=save_state,
            page_event_filter=None,
            event_to_insert=lambda e: e,
            seen_db=None,
            insert_batch_size=1000,
            insert_max_bytes=None,
            rewrite_event_in_place=None,
            insert_events=insert_events,
            is_http_413=lambda _: False,
            on_single_413=None,
            incr_fetched=lambda _: None,
            incr_inserted=lambda _: None,
            incr_inserted_bytes=lambda _: None,
            incr_skipped_deleted=None,
            incr_skipped_seen=None,
            incr_attachments_copied=None,
            pipeline=True,
        )

        # save_state should have been called after each page + end-of-stream.
        # pk should advance: pk1, pk2, then end-of-stream save with pk2.
        assert "pk1" in save_state_pks
        assert "pk2" in save_state_pks

    async def test_pipeline_insert_error_cancels_prefetch(self):
        """If insert fails, the prefetch task should be cancelled cleanly."""
        pages = [
            [{"id": "a", "_pagination_key": "pk1"}],
            [{"id": "b", "_pagination_key": "pk2"}],
        ]
        run, state = _build_streaming_harness(
            pages, pipeline=True, fail_on_insert_page=0
        )

        with pytest.raises(RuntimeError, match="insert failed"):
            await run()

        # The first insert failed; the second page may have been prefetched
        # but its insert should NOT have happened.
        assert len(state["insert_calls"]) == 0  # insert_calls only appended on success

    async def test_pipeline_empty_pages(self):
        """Pipeline should handle empty input gracefully."""
        run, state = _build_streaming_harness([], pipeline=True)
        await run()

        assert state["fetch_count"]["n"] == 0
        assert state["insert_count"]["n"] == 0

    async def test_pipeline_single_page(self):
        """Pipeline with only one data page should work fine."""
        pages = [[{"id": "x", "_pagination_key": "pk_only"}]]
        run, state = _build_streaming_harness(pages, pipeline=True)
        await run()

        assert state["fetch_count"]["n"] == 1
        assert state["insert_count"]["n"] == 1
        assert state["pk"]["value"] == "pk_only"
