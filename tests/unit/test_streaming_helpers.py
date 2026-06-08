"""Unit tests for the shared streaming helpers in streaming_utils.

These were previously duplicated as private staticmethods/methods across the
logs, dataset, and experiment migrators. They are now single shared functions;
this pins their behavior so the three call sites stay consistent.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx

from braintrust_migrate.streaming_utils import (
    EventsStreamState,
    approx_event_size_bytes,
    build_stream_progress,
    count_attachment_refs,
    dump_oversize_event_summary,
    is_http_413,
    make_stream_progress_hooks,
)


def _http_status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://api.example/logs3")
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError("boom", request=request, response=response)


def test_is_http_413():
    assert is_http_413(_http_status_error(413)) is True
    assert is_http_413(_http_status_error(400)) is False
    assert is_http_413(ValueError("nope")) is False


def test_approx_event_size_bytes():
    assert approx_event_size_bytes({"a": "hello"}) == len(
        json.dumps({"a": "hello"}, separators=(",", ":"))
    )
    # Non-serializable values return None rather than raising.
    assert approx_event_size_bytes({"a": {1, 2, 3}}) is None


def test_count_attachment_refs_walks_nested_structures():
    ref = {"type": "braintrust_attachment", "key": "k", "filename": "f", "content_type": "application/json"}
    event = {
        "input": {"a": ref, "b": [ref, {"c": ref}]},  # 3 refs
        "metadata": {"type": "braintrust_attachment"},  # missing key -> not counted
        "output": "plain",
    }
    assert count_attachment_refs(event) == 3


class _RecordingLogger:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def error(self, message: str, **kwargs: Any) -> None:
        self.calls.append((message, kwargs))


def test_dump_oversize_event_summary_writes_file_and_logs(tmp_path: Path):
    logger = _RecordingLogger()
    event = {
        "id": "evt-1",
        "root_span_id": "rs",
        "span_id": "sp",
        "created": "2023-01-01T00:00:00Z",
        "input": {"big": "x" * 100},
    }
    dump_oversize_event_summary(
        out_dir=tmp_path,
        filename_prefix="oversize_dataset_event_",
        event_label="dataset event",
        dest_id_field="dest_dataset_id",
        dest_id_value="dest-123",
        cursor="cur",
        event=event,
        error=RuntimeError("too big"),
        logger=logger,
    )

    path = tmp_path / "oversize_dataset_event_evt-1.json"
    assert path.exists()
    summary = json.loads(path.read_text())
    # Per-resource dest-id key name is preserved (not flattened to a generic key).
    assert summary["dest_dataset_id"] == "dest-123"
    assert summary["event_id"] == "evt-1"
    assert summary["cursor"] == "cur"
    assert summary["attachment_refs"] == 0
    assert summary["top_level_keys"] == sorted(event.keys())

    assert len(logger.calls) == 1
    message, kwargs = logger.calls[0]
    # event_label drives the message wording.
    assert message.startswith("Oversize dataset event isolated (413).")
    assert kwargs["event_id"] == "evt-1"


def test_dump_oversize_event_summary_unknown_id_and_prefix(tmp_path: Path):
    logger = _RecordingLogger()
    dump_oversize_event_summary(
        out_dir=tmp_path,
        filename_prefix="oversize_project_logs_event_",
        event_label="event",
        dest_id_field="dest_project_id",
        dest_id_value="p1",
        cursor=None,
        event={"input": "x"},  # no id
        error=RuntimeError("x"),
        logger=logger,
    )
    assert (tmp_path / "oversize_project_logs_event_unknown.json").exists()
    assert logger.calls[0][0].startswith("Oversize event isolated (413).")


def _events_state() -> EventsStreamState:
    return EventsStreamState(
        btql_min_pagination_key="0123456789ABCDEFGHIJ",  # 20 chars
        fetched_events=10,
        inserted_events=8,
        inserted_bytes=100,
        skipped_deleted=1,
        skipped_seen=2,
        attachments_copied=3,
    )


def test_build_stream_progress_fetch_uses_info_and_truncates_cursor():
    state = _events_state()
    info = {
        "page_num": 2,
        "page_events": 50,
        "fetched_total": 7,
        "inserted_total": 5,
        "inserted_bytes_total": 60,
        "skipped_deleted_total": 0,
        "skipped_seen_total": 1,
        "attachments_copied_total": 0,
        "pending_buffered_rows": 3,
        "pending_buffered_bytes": 30,
    }
    p = build_stream_progress(
        "fetch",
        info,
        state,
        resource="dataset_events",
        id_fields={"source_dataset_ids": ["s"], "dest_dataset_ids": ["d"]},
    )
    assert p["resource"] == "dataset_events"
    assert p["phase"] == "fetch"
    assert p["source_dataset_ids"] == ["s"]
    assert p["dest_dataset_ids"] == ["d"]
    # fetch/page totals come from info, not state.
    assert p["fetched_total"] == 7
    assert p["pending_buffered_rows"] == 3
    # cursor is the first 16 chars + ellipsis.
    assert p["cursor"] == "0123456789ABCDEF…"
    assert p["next_cursor"] is None


def test_build_stream_progress_insert_uses_state_totals():
    state = _events_state()
    info = {
        "inserted_last": 4,
        "inserted_bytes_last": 40,
        "insert_seconds": 0.5,
        "flush_rows": 4,
        "flush_buffer_bytes": 40,
    }
    p = build_stream_progress(
        "insert",
        info,
        state,
        resource="experiment_events",
        id_fields={"source_experiment_ids": ["s"]},
    )
    assert p["phase"] == "insert"
    # insert totals come from committed state, not the per-batch info.
    assert p["fetched_total"] == state.fetched_events
    assert p["inserted_total"] == state.inserted_events
    assert p["inserted_bytes_total"] == state.inserted_bytes
    assert p["page_num"] is None
    assert p["pending_buffered_rows"] == 0
    assert p["inserted_last"] == 4
    assert p["insert_seconds"] == 0.5


def test_build_stream_progress_done_nulls_cursor():
    state = _events_state()
    info = {"fetched_total": 10, "inserted_total": 8, "inserted_bytes_total": 100}
    p = build_stream_progress(
        "done", info, state, resource="dataset_events", id_fields={}
    )
    assert p["phase"] == "done"
    assert p["fetched_total"] == 10
    assert p["cursor"] is None


def test_make_stream_progress_hooks_none_passthrough():
    assert (
        make_stream_progress_hooks(None, _events_state(), resource="x", id_fields={})
        is None
    )


def test_make_stream_progress_hooks_emits_via_callback():
    captured: list[dict] = []
    hooks = make_stream_progress_hooks(
        captured.append,
        _events_state(),
        resource="dataset_events",
        id_fields={"source_dataset_ids": ["s"]},
    )
    assert set(hooks) == {"on_fetch", "on_page", "on_insert", "on_done"}
    hooks["on_fetch"]({"page_num": 1})
    assert captured[-1]["phase"] == "fetch"
    assert captured[-1]["resource"] == "dataset_events"
    hooks["on_done"]({})
    assert captured[-1]["phase"] == "done"
