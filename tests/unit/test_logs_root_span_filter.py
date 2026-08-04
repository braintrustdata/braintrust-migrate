"""Tests for trace-level routing of project logs by root span name.

The filter exists so one source project can be split across two destination
projects. The property that matters most is that include and exclude are exact
complements: every span lands in exactly one of the two runs.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest

import braintrust_migrate.resources.logs as logs_module
from braintrust_migrate.config import MigrationConfig
from braintrust_migrate.resources.logs import LogsMigrator
from braintrust_migrate.streaming_utils import build_btql_sorted_page_query

ROOT_NAME = "chat.message"


def _span(
    event_id: str,
    span_id: str,
    root_span_id: str,
    name: str,
    pagination_key: str,
) -> dict[str, Any]:
    return {
        "id": event_id,
        "span_id": span_id,
        "root_span_id": root_span_id,
        "span_attributes": {"name": name},
        "_pagination_key": pagination_key,
        "_xact_id": "1",
        "created": "2026-01-01T00:00:00Z",
    }


# Two matching traces (t1 with children, t3 single-span) and one non-matching
# trace (t2 with a child). Mirrors the real shape: children do not carry the
# root's name, so only the prepass can associate them.
ALL_SPANS: list[dict[str, Any]] = [
    _span("e1", "s1", "s1", ROOT_NAME, "pk1"),
    _span("e2", "s2", "s1", "llm.call", "pk2"),
    _span("e3", "s3", "s1", "tool.lookup", "pk3"),
    _span("e4", "s4", "s4", "other-root", "pk4"),
    _span("e5", "s5", "s4", "llm.call", "pk5"),
    _span("e6", "s6", "s6", ROOT_NAME, "pk6"),
]

MATCHING_IDS = {"e1", "e2", "e3", "e6"}
NON_MATCHING_IDS = {"e4", "e5"}


class _StubClient:
    """Serves BTQL prepass and streaming pages, and records inserts."""

    def __init__(
        self,
        *,
        pages: list[list[dict[str, Any]]],
        migration_config: MigrationConfig,
    ) -> None:
        self.pages = pages
        self.migration_config = migration_config
        self.inserts: list[list[dict[str, Any]]] = []
        self.prepass_queries = 0

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

    def _page_index_for_query(self, query: str) -> int:
        match = re.search(r"_pagination_key > '([^']+)'", query)
        if match is None:
            return 0
        key = match.group(1)
        for index, page in enumerate(self.pages):
            if page and page[-1]["_pagination_key"] == key:
                return index + 1
        return len(self.pages)

    async def raw_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        timeout: float | None = None,
    ) -> Any:
        _ = params, timeout
        assert method.lower() == "post"
        assert path == "/btql"
        assert json is not None
        query = json["query"]
        assert isinstance(query, str)

        if "span_attributes.name" in query:
            self.prepass_queries += 1
            if "_pagination_key >" in query:
                return {"data": []}
            rows = [
                {
                    "span_id": span["span_id"],
                    "root_span_id": span["root_span_id"],
                    "_pagination_key": span["_pagination_key"],
                }
                for page in self.pages
                for span in page
                if span["span_attributes"]["name"] == ROOT_NAME
            ]
            return {"data": rows}

        index = self._page_index_for_query(query)
        if index >= len(self.pages):
            return {"data": []}
        return {"data": self.pages[index]}


class _FakeSDKProjectLogsWriter:
    def __init__(self, dest_client: _StubClient, project_id: str) -> None:
        self._dest_client = dest_client
        self._project_id = project_id

    async def write_rows(self, rows: list[dict[str, Any]]) -> None:
        self._dest_client.inserts.append([dict(row) for row in rows])


async def _run_migration(
    tmp_path: Path,
    *,
    pages: list[list[dict[str, Any]]],
    config: MigrationConfig,
) -> tuple[list[str], LogsMigrator, _StubClient]:
    source = _StubClient(pages=pages, migration_config=config)
    dest = _StubClient(pages=pages, migration_config=config)

    original_writer = logs_module.SDKProjectLogsWriter
    logs_module.SDKProjectLogsWriter = _FakeSDKProjectLogsWriter
    try:
        migrator = LogsMigrator(
            source,  # type: ignore[arg-type]
            dest,  # type: ignore[arg-type]
            tmp_path,
            page_limit=10,
            use_seen_db=False,
        )
        migrator.set_destination_project_id("dest-project-id")
        await migrator.migrate_all("source-project-id")
    finally:
        logs_module.SDKProjectLogsWriter = original_writer

    inserted = [event["id"] for batch in dest.inserts for event in batch]
    return inserted, migrator, source


@pytest.mark.asyncio
async def test_include_mode_migrates_whole_matching_traces(tmp_path: Path) -> None:
    """Children of a matching root come along even though their names differ."""
    config = MigrationConfig(logs_include_root_span_name=ROOT_NAME)
    inserted, _, _ = await _run_migration(
        tmp_path, pages=[list(ALL_SPANS)], config=config
    )

    assert set(inserted) == MATCHING_IDS
    # e2/e3 are named llm.call and tool.lookup; they qualify only via root_span_id.
    assert "e2" in inserted
    assert "e3" in inserted


@pytest.mark.asyncio
async def test_exclude_mode_migrates_everything_else(tmp_path: Path) -> None:
    config = MigrationConfig(logs_exclude_root_span_name=ROOT_NAME)
    inserted, _, _ = await _run_migration(
        tmp_path, pages=[list(ALL_SPANS)], config=config
    )

    assert set(inserted) == NON_MATCHING_IDS


@pytest.mark.asyncio
async def test_include_and_exclude_are_exact_complements(tmp_path: Path) -> None:
    """The core guarantee for a paired split: no span duplicated, none dropped."""
    include_inserted, _, _ = await _run_migration(
        tmp_path / "include",
        pages=[list(ALL_SPANS)],
        config=MigrationConfig(logs_include_root_span_name=ROOT_NAME),
    )
    exclude_inserted, _, _ = await _run_migration(
        tmp_path / "exclude",
        pages=[list(ALL_SPANS)],
        config=MigrationConfig(logs_exclude_root_span_name=ROOT_NAME),
    )

    assert set(include_inserted) & set(exclude_inserted) == set()
    assert set(include_inserted) | set(exclude_inserted) == {
        span["id"] for span in ALL_SPANS
    }


@pytest.mark.asyncio
async def test_no_filter_migrates_everything(tmp_path: Path) -> None:
    inserted, _, source = await _run_migration(
        tmp_path, pages=[list(ALL_SPANS)], config=MigrationConfig()
    )

    assert set(inserted) == {span["id"] for span in ALL_SPANS}
    assert source.prepass_queries == 0, "prepass must not run without a filter"


@pytest.mark.asyncio
async def test_pagination_advances_through_fully_filtered_page(
    tmp_path: Path,
) -> None:
    """A page where every row is filtered out must still advance the cursor."""
    page1 = [ALL_SPANS[3], ALL_SPANS[4]]  # e4, e5 -- entirely non-matching
    page2 = [ALL_SPANS[0], ALL_SPANS[1]]  # e1, e2 -- matching trace

    config = MigrationConfig(logs_include_root_span_name=ROOT_NAME)
    inserted, migrator, _ = await _run_migration(
        tmp_path, pages=[page1, page2], config=config
    )

    assert set(inserted) == {"e1", "e2"}
    state = migrator._stream_state
    assert state.btql_min_pagination_key == "pk2", (
        "cursor must advance past the fully filtered page"
    )
    assert state.skipped_filtered == len(page1)
    assert state.fetched_events == len(page1) + len(page2), (
        "fetched counts pre-filter rows"
    )


@pytest.mark.asyncio
async def test_include_mode_with_zero_matches_migrates_nothing(
    tmp_path: Path,
) -> None:
    pages = [[ALL_SPANS[3], ALL_SPANS[4]]]  # no span carries the root name
    config = MigrationConfig(logs_include_root_span_name="does-not-exist")
    inserted, _, _ = await _run_migration(tmp_path, pages=pages, config=config)

    assert inserted == []


@pytest.mark.asyncio
async def test_checkpoint_rejects_changed_filter(tmp_path: Path) -> None:
    """Resuming with a different filter would corrupt the split; it must fail."""
    await _run_migration(
        tmp_path,
        pages=[list(ALL_SPANS)],
        config=MigrationConfig(logs_include_root_span_name=ROOT_NAME),
    )

    with pytest.raises(ValueError, match="mismatch vs checkpoint"):
        await _run_migration(
            tmp_path,
            pages=[list(ALL_SPANS)],
            config=MigrationConfig(logs_include_root_span_name="some-other-name"),
        )


@pytest.mark.asyncio
async def test_checkpoint_rejects_dropped_filter(tmp_path: Path) -> None:
    await _run_migration(
        tmp_path,
        pages=[list(ALL_SPANS)],
        config=MigrationConfig(logs_include_root_span_name=ROOT_NAME),
    )

    with pytest.raises(ValueError, match="but this run does not"):
        await _run_migration(
            tmp_path, pages=[list(ALL_SPANS)], config=MigrationConfig()
        )


def test_trace_id_falls_back_when_root_span_id_missing() -> None:
    assert (
        LogsMigrator._trace_id_for_event({"root_span_id": "r", "span_id": "s"}) == "r"
    )
    assert LogsMigrator._trace_id_for_event({"span_id": "s", "id": "e"}) == "s"
    assert LogsMigrator._trace_id_for_event({"id": "e"}) == "e"
    assert LogsMigrator._trace_id_for_event({}) is None


def test_build_query_appends_extra_conditions() -> None:
    query = build_btql_sorted_page_query(
        from_expr="project_logs('p') spans",
        limit=10,
        last_pagination_key="pk1",
        created_after="2026-01-01T00:00:00Z",
        extra_conditions=["span_attributes.name = 'chat.message'"],
    )

    assert "span_attributes.name = 'chat.message'" in query
    assert "created >= '2026-01-01T00:00:00Z'" in query
    assert "_pagination_key > 'pk1'" in query


def test_config_rejects_both_filters() -> None:
    with pytest.raises(ValueError, match="Set only one of"):
        MigrationConfig(
            logs_include_root_span_name="a",
            logs_exclude_root_span_name="b",
        )
