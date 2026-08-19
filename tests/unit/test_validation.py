"""Unit tests for post-migration validation (object parity + event count-parity)."""

from __future__ import annotations

from typing import Any

import pytest

from braintrust_migrate.validation import count_events, validate_resource


class _FakeClient:
    """Stub client: serves resource lists and BTQL count queries."""

    def __init__(
        self,
        *,
        lists: dict[str, list[dict[str, Any]]] | None = None,
        counts: dict[str, int] | None = None,
    ) -> None:
        self._lists = lists or {}
        self._counts = counts or {}  # parent_id -> event count

    async def with_retry(self, _op, coro_func, **kwargs):
        res = coro_func()
        return await res if hasattr(res, "__await__") else res

    async def raw_request(self, method, path, *, json=None, params=None, **kwargs):
        assert path == "/btql"
        query = json["query"]
        for parent_id, n in self._counts.items():
            if f"'{parent_id}'" in query:
                return {"data": [{"n": n}]}
        return {"data": [{"n": 0}]}


async def _list_fn(client, resource_type, project_id):
    return client._lists.get(resource_type, [])


async def test_out_of_scope_resource_returns_none():
    src = _FakeClient()
    dst = _FakeClient()
    result = await validate_resource(
        "roles",
        source_client=src,
        dest_client=dst,
        source_project_id="sp",
        dest_project_id="dp",
        list_fn=_list_fn,
    )
    assert result is None


async def test_object_parity_reports_specific_missing():
    src = _FakeClient(
        lists={
            "prompts": [
                {"id": "s1", "name": "A"},
                {"id": "s2", "name": "B"},
                {"id": "s3", "name": "C"},
            ]
        }
    )
    dst = _FakeClient(lists={"prompts": [{"id": "d1", "name": "A"}, {"id": "d2", "name": "B"}]})

    result = await validate_resource(
        "prompts",
        source_client=src,
        dest_client=dst,
        source_project_id="sp",
        dest_project_id="dp",
        list_fn=_list_fn,
    )
    assert result is not None and result.ok is False
    (check,) = result.checks
    assert check.kind == "object"
    assert check.source_count == 3
    assert check.dest_count == 2
    assert check.missing == ["C"]


async def test_function_bundle_excluded_from_expected():
    # Source has an inline fn (migratable) and a bundle fn (intentionally skipped).
    src = _FakeClient(
        lists={
            "functions": [
                {
                    "id": "s1",
                    "slug": "inline-fn",
                    "function_data": {"type": "code", "data": {"type": "inline", "code": "x"}},
                },
                {
                    "id": "s2",
                    "slug": "bundle-fn",
                    "function_data": {"type": "code", "data": {"type": "bundle"}},
                },
            ]
        }
    )
    # Dest only has the inline one — the bundle one is correctly absent.
    dst = _FakeClient(lists={"functions": [{"id": "d1", "slug": "inline-fn"}]})

    result = await validate_resource(
        "functions",
        source_client=src,
        dest_client=dst,
        source_project_id="sp",
        dest_project_id="dp",
        list_fn=_list_fn,
    )
    assert result is not None and result.ok is True
    (check,) = result.checks
    assert check.source_count == 1  # only the inline fn is expected
    assert check.missing == []


async def test_dataset_object_and_event_parity_match():
    src = _FakeClient(
        lists={"datasets": [{"id": "sd1", "name": "D1"}]}, counts={"sd1": 50}
    )
    dst = _FakeClient(
        lists={"datasets": [{"id": "dd1", "name": "D1"}]}, counts={"dd1": 50}
    )

    result = await validate_resource(
        "datasets",
        source_client=src,
        dest_client=dst,
        source_project_id="sp",
        dest_project_id="dp",
        list_fn=_list_fn,
        id_mapping={"sd1": "dd1"},
    )
    assert result is not None and result.ok is True
    kinds = {c.kind for c in result.checks}
    assert kinds == {"object", "events"}
    event_check = next(c for c in result.checks if c.kind == "events")
    assert event_check.scope == "datasets:D1/events"
    assert event_check.source_count == 50 and event_check.dest_count == 50


async def test_dataset_event_count_mismatch_flags_failure():
    src = _FakeClient(
        lists={"datasets": [{"id": "sd1", "name": "D1"}]}, counts={"sd1": 50}
    )
    dst = _FakeClient(
        lists={"datasets": [{"id": "dd1", "name": "D1"}]}, counts={"dd1": 48}
    )

    result = await validate_resource(
        "datasets",
        source_client=src,
        dest_client=dst,
        source_project_id="sp",
        dest_project_id="dp",
        list_fn=_list_fn,
        id_mapping={"sd1": "dd1"},
    )
    assert result is not None and result.ok is False
    event_check = next(c for c in result.checks if c.kind == "events")
    assert event_check.source_count == 50 and event_check.dest_count == 48
    assert event_check.ok is False


async def test_logs_event_parity_project_level():
    src = _FakeClient(counts={"sp": 1000})
    dst = _FakeClient(counts={"dp": 1000})

    result = await validate_resource(
        "logs",
        source_client=src,
        dest_client=dst,
        source_project_id="sp",
        dest_project_id="dp",
        list_fn=_list_fn,
    )
    assert result is not None and result.ok is True
    (check,) = result.checks
    assert check.kind == "events" and check.scope == "logs/events"
    assert check.source_count == 1000 and check.dest_count == 1000


async def test_count_events_honors_created_window():
    captured: dict[str, str] = {}

    class _CaptureClient(_FakeClient):
        async def raw_request(self, method, path, *, json=None, params=None, **kwargs):
            captured["query"] = json["query"]
            return {"data": [{"n": 7}]}

    client = _CaptureClient()
    n = await count_events(
        client,
        "project_logs('p') spans",
        created_after="2026-01-01T00:00:00Z",
        created_before="2026-02-01T00:00:00Z",
    )
    assert n == 7
    assert "created >= '2026-01-01T00:00:00Z'" in captured["query"]
    assert "created < '2026-02-01T00:00:00Z'" in captured["query"]
    assert "count(1)" in captured["query"]
