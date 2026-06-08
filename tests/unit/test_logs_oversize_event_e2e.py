"""End-to-end-ish test of the logs migrator spilling an oversized event.

Drives the real LogsMigrator.migrate_all loop over a page of 5 events, one of
which is larger than the 20MB per-span logging limit. Source BTQL and the
destination attachment-upload handshake are mocked; the SDK logs writer is
stubbed to capture exactly the rows that would be sent to /logs3. We then assert
the oversized row was spilled to an attachment and every row is under the cap.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

import braintrust_migrate.resources.logs as logs_module
from braintrust_migrate.resources.logs import LogsMigrator

# The SaaS cap (LOGS_MAX_SPAN_BYTES) that produced the original 400 on
# /logs3/overflow. Confirmed in braintrust api-ts (LOGS_MAX_SPAN_MB=20).
LOGS_MAX_SPAN_BYTES = 20 * 1024 * 1024


class _SourceStub:
    """Returns BTQL pages, one list of events per /btql call."""

    def __init__(self, btql_pages: list[list[dict[str, Any]]]) -> None:
        self._btql_pages = btql_pages

    async def with_retry(self, _op: str, fn, *, non_retryable_statuses=None):
        _ = non_retryable_statuses
        return await fn()

    async def raw_request(
        self, method: str, path: str, *, params=None, json=None, timeout=None
    ) -> Any:
        _ = params, json, timeout
        assert method.upper() == "POST"
        if path == "/btql":
            data = self._btql_pages.pop(0) if self._btql_pages else []
            return {"data": data}
        raise AssertionError(f"Unexpected source path: {path}")


class _DestStub:
    """Handles the attachment upload handshake and records uploaded bytes.

    - raw_request: /attachment (signed URL), /attachment/status (mark done)
    - _http_client: httpx mock transport for the PUT to the signed object URL
    - inserted_rows: populated by the stubbed SDK logs writer (the /logs3 path)
    """

    _UPLOAD_URL = "https://upload.example/put"

    def __init__(self) -> None:
        self.attachment_uploads: list[bytes] = []
        self.inserted_rows: list[dict[str, Any]] = []
        self.meta_calls = 0
        self.status_calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            if str(request.url) == self._UPLOAD_URL:
                assert request.method == "PUT"
                assert request.headers.get("Authorization") is None
                self.attachment_uploads.append(request.content)
                return httpx.Response(200)
            raise AssertionError(f"Unexpected dest URL: {request.method} {request.url}")

        self._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def get_org_id(self) -> str:
        return "org_dest"

    async def with_retry(self, _op: str, fn, *, non_retryable_statuses=None):
        _ = non_retryable_statuses
        return await fn()

    async def raw_request(
        self, method: str, path: str, *, params=None, json=None, timeout=None
    ) -> Any:
        _ = params, timeout
        assert method.upper() == "POST"
        if path == "/attachment":
            self.meta_calls += 1
            assert json["content_type"] == "application/json"
            return {
                "signedUrl": self._UPLOAD_URL,
                "headers": {"Content-Type": "application/json"},
            }
        if path == "/attachment/status":
            self.status_calls += 1
            assert json["status"] == {"upload_status": "done"}
            return {}
        raise AssertionError(f"Unexpected dest path: {path}")


class _FakeSDKProjectLogsWriter:
    def __init__(self, dest_client: _DestStub, project_id: str) -> None:
        self._dest_client = dest_client
        self._project_id = project_id

    async def write_rows(self, rows: list[dict[str, Any]]) -> None:
        self._dest_client.inserted_rows.extend(dict(row) for row in rows)


@pytest.mark.asyncio
async def test_oversized_event_among_normal_events_is_spilled(tmp_path: Path) -> None:
    # One ~21MB event (over the 20MB cap) mixed with four small ones.
    big_input = {"transcript": "x" * (21 * 1024 * 1024)}
    events: list[dict[str, Any]] = [
        {
            "id": f"e{i}",
            "_pagination_key": f"p{i}",
            "_xact_id": str(10 + i),
            "created": f"2023-01-01T00:00:0{i}Z",
            "input": {"q": f"hello-{i}"},
        }
        for i in range(5)
    ]
    events[2]["input"] = big_input
    # Sanity: the oversized event really would exceed the span cap as-is.
    assert (
        len(json.dumps(events[2], ensure_ascii=False).encode("utf-8"))
        > LOGS_MAX_SPAN_BYTES
    )

    source = _SourceStub(btql_pages=[events])
    dest = _DestStub()

    original_writer = logs_module.SDKProjectLogsWriter
    logs_module.SDKProjectLogsWriter = _FakeSDKProjectLogsWriter
    try:
        migrator = LogsMigrator(
            source,  # type: ignore[arg-type]
            dest,  # type: ignore[arg-type]
            tmp_path,
            page_limit=10,
            insert_batch_size=10,
            use_version_snapshot=False,
            use_seen_db=True,
        )
        migrator.set_destination_project_id("dest-project")
        result = await migrator.migrate_all("source-project")
    finally:
        logs_module.SDKProjectLogsWriter = original_writer
        await dest._http_client.aclose()

    # All five migrated; exactly one field spilled.
    assert result["migrated"] == 5
    assert result["total"] == 5
    assert result["spilled_fields"] == 1
    assert not result["errors"]

    # Exactly one attachment uploaded, and its bytes are the original big value.
    assert len(dest.attachment_uploads) == 1
    assert dest.meta_calls == 1 and dest.status_calls == 1
    assert json.loads(dest.attachment_uploads[0].decode("utf-8")) == big_input

    by_id = {row["id"]: row for row in dest.inserted_rows}
    assert set(by_id) == {"e0", "e1", "e2", "e3", "e4"}

    # The oversized event's input is now an attachment reference...
    spilled_input = by_id["e2"]["input"]
    assert spilled_input["type"] == "braintrust_attachment"
    assert spilled_input["filename"] == "input.json"
    assert spilled_input["content_type"] == "application/json"

    # ...and every row actually handed to /logs3 is under the real 20MB cap.
    for row in dest.inserted_rows:
        row_bytes = len(json.dumps(row, ensure_ascii=False).encode("utf-8"))
        assert row_bytes < LOGS_MAX_SPAN_BYTES

    # The four normal events are untouched (no needless spilling/uploads).
    for i in (0, 1, 3, 4):
        assert by_id[f"e{i}"]["input"] == {"q": f"hello-{i}"}
