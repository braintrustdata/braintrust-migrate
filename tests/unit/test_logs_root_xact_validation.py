from __future__ import annotations

from typing import Any

import pytest

from braintrust_migrate.validation.logs import validate_logs_root_xacts

EXPECTED_DUPLICATE_DESTINATION_ROWS = 2


class _BtqlClient:
    def __init__(self, pages_by_project: dict[str, list[list[dict[str, Any]]]]) -> None:
        self.pages_by_project = pages_by_project
        self.queries: list[str] = []

    async def with_retry(
        self,
        _operation_name: str,
        coro_func,
        *,
        non_retryable_statuses: set[int] | None = None,
    ):
        _ = non_retryable_statuses
        res = coro_func()
        if hasattr(res, "__await__"):
            return await res
        return res

    async def raw_request(
        self, method: str, path: str, *, json: Any = None, **kwargs: Any
    ) -> Any:
        _ = kwargs
        assert method.upper() == "POST"
        assert path == "/btql"
        query = (json or {}).get("query")
        assert isinstance(query, str)
        assert "is_root = true" in query
        self.queries.append(query)

        for project_id, pages in self.pages_by_project.items():
            if f"project_logs('{project_id}') spans" in query:
                if pages:
                    return {"data": pages.pop(0)}
                return {"data": []}

        raise AssertionError(f"Unexpected query: {query}")


def _row(
    *,
    row_id: str,
    root_span_id: str | None = "root-1",
    xact_id: str | None = "10",
    pagination_key: str = "pk1",
    origin: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": row_id,
        "_pagination_key": pagination_key,
        "is_root": True,
        "created": "2026-01-01T00:00:00Z",
    }
    if root_span_id is not None:
        row["root_span_id"] = root_span_id
    if xact_id is not None:
        row["_xact_id"] = xact_id
    if origin is not None:
        row["origin"] = origin
    return row


def _origin(source_project_id: str, xact_id: str | None = "10") -> dict[str, Any]:
    origin: dict[str, Any] = {
        "object_type": "project_logs",
        "object_id": source_project_id,
        "id": "source-row",
    }
    if xact_id is not None:
        origin["_xact_id"] = xact_id
    return origin


@pytest.mark.asyncio
async def test_logs_root_xact_validation_passes_on_matching_origin_xact() -> None:
    source = _BtqlClient({"src": [[_row(row_id="source-row")]]})
    dest = _BtqlClient({"dst": [[_row(row_id="dest-row", origin=_origin("src"))]]})

    summary = await validate_logs_root_xacts(
        source_client=source,  # type: ignore[arg-type]
        dest_client=dest,  # type: ignore[arg-type]
        projects=[{"name": "Project A", "source_id": "src", "dest_id": "dst"}],
        page_limit=100,
    )

    assert summary.success is True
    assert summary.checked == 1
    assert summary.matched == 1
    assert summary.missing_count == 0


@pytest.mark.asyncio
async def test_logs_root_xact_validation_reports_missing_destination() -> None:
    source = _BtqlClient({"src": [[_row(row_id="source-row")]]})
    dest = _BtqlClient({"dst": [[]]})

    summary = await validate_logs_root_xacts(
        source_client=source,  # type: ignore[arg-type]
        dest_client=dest,  # type: ignore[arg-type]
        projects=[{"name": "Project A", "source_id": "src", "dest_id": "dst"}],
        page_limit=100,
    )

    project = summary.projects[0]
    assert summary.success is False
    assert len(project.missing or []) == 1
    assert (project.missing or [])[0]["root_span_id"] == "root-1"


@pytest.mark.asyncio
async def test_logs_root_xact_validation_reports_duplicate_destination_matches() -> None:
    source = _BtqlClient({"src": [[_row(row_id="source-row")]]})
    dest = _BtqlClient(
        {
            "dst": [
                [
                    _row(row_id="dest-row-1", origin=_origin("src")),
                    _row(
                        row_id="dest-row-2",
                        origin=_origin("src"),
                        pagination_key="pk2",
                    ),
                ]
            ]
        }
    )

    summary = await validate_logs_root_xacts(
        source_client=source,  # type: ignore[arg-type]
        dest_client=dest,  # type: ignore[arg-type]
        projects=[{"name": "Project A", "source_id": "src", "dest_id": "dst"}],
        page_limit=100,
    )

    project = summary.projects[0]
    assert summary.success is False
    assert len(project.duplicate_destination or []) == 1
    assert (
        project.duplicate_destination or []
    )[0]["count"] == EXPECTED_DUPLICATE_DESTINATION_ROWS


@pytest.mark.asyncio
async def test_logs_root_xact_validation_ignores_unrelated_destination_origin() -> None:
    source = _BtqlClient({"src": [[_row(row_id="source-row")]]})
    dest = _BtqlClient(
        {
            "dst": [
                [
                    _row(
                        row_id="dest-row",
                        origin=_origin("different-source"),
                    )
                ]
            ]
        }
    )

    summary = await validate_logs_root_xacts(
        source_client=source,  # type: ignore[arg-type]
        dest_client=dest,  # type: ignore[arg-type]
        projects=[{"name": "Project A", "source_id": "src", "dest_id": "dst"}],
        page_limit=100,
    )

    project = summary.projects[0]
    assert summary.success is False
    assert len(project.missing or []) == 1
    assert project.unverifiable_destination == 0


@pytest.mark.asyncio
async def test_logs_root_xact_validation_reports_unverifiable_rows() -> None:
    source = _BtqlClient({"src": [[_row(row_id="source-row", xact_id=None)]]})
    dest = _BtqlClient({"dst": [[_row(row_id="dest-row", origin=_origin("src", None))]]})

    summary = await validate_logs_root_xacts(
        source_client=source,  # type: ignore[arg-type]
        dest_client=dest,  # type: ignore[arg-type]
        projects=[{"name": "Project A", "source_id": "src", "dest_id": "dst"}],
        page_limit=100,
    )

    project = summary.projects[0]
    assert summary.success is False
    assert project.unverifiable_source == 1
    assert project.unverifiable_destination == 1


@pytest.mark.asyncio
async def test_logs_root_xact_validation_skips_projects_without_destination() -> None:
    source = _BtqlClient({"src": [[_row(row_id="source-row")]]})
    dest = _BtqlClient({"dst": [[_row(row_id="dest-row", origin=_origin("src"))]]})

    summary = await validate_logs_root_xacts(
        source_client=source,  # type: ignore[arg-type]
        dest_client=dest,  # type: ignore[arg-type]
        projects=[{"name": "Project A", "source_id": "src", "dest_id": ""}],
        page_limit=100,
    )

    assert summary.success is True
    assert summary.skipped_projects == 1
    assert source.queries == []
    assert dest.queries == []
