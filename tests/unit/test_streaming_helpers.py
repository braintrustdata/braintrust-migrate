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
    approx_event_size_bytes,
    count_attachment_refs,
    dump_oversize_event_summary,
    is_http_413,
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
