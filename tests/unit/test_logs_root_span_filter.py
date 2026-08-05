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
    span_parents: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "id": event_id,
        "span_id": span_id,
        "root_span_id": root_span_id,
        "span_parents": span_parents,
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
    _span("e2", "s2", "s1", "llm.call", "pk2", span_parents=["s1"]),
    _span("e3", "s3", "s1", "tool.lookup", "pk3", span_parents=["s1"]),
    _span("e4", "s4", "s4", "other-root", "pk4"),
    _span("e5", "s5", "s4", "llm.call", "pk5", span_parents=["s4"]),
    _span("e6", "s6", "s6", ROOT_NAME, "pk6"),
]

# OpenTelemetry-style ingestion: `root_span_id` is a 16-byte trace id and
# `span_id` is an 8-byte span id, so they never match even for top-level spans.
# Top-level-ness is carried solely by an empty `span_parents`. A single trace
# can hold several top-level spans of the same name (one per chat turn).
OTEL_TRACE = "b3b9b3f191d3d56ccc92b4023a321c02"
OTEL_OTHER_TRACE = "a932535782befd370e423c2d7ef93af5"
OTEL_SPANS: list[dict[str, Any]] = [
    _span("o1", "0d2ddc17e6aadaff", OTEL_TRACE, ROOT_NAME, "qk1"),
    _span(
        "o2",
        "f2d2575b5b3db975",
        OTEL_TRACE,
        "llm.stream",
        "qk2",
        span_parents=["0d2ddc17e6aadaff"],
    ),
    _span("o3", "9814bb66fbb565ff", OTEL_TRACE, ROOT_NAME, "qk3"),
    _span(
        "o4",
        "b84e2143935e5bf3",
        OTEL_TRACE,
        "llm.stream",
        "qk4",
        span_parents=["9814bb66fbb565ff"],
    ),
    _span("o5", "94b27b8d9f5d224f", OTEL_OTHER_TRACE, "recommendation", "qk5"),
    _span(
        "o6",
        "c87905908e5749fb",
        OTEL_OTHER_TRACE,
        "llm.stream",
        "qk6",
        span_parents=["94b27b8d9f5d224f"],
    ),
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
                    "span_parents": span.get("span_parents"),
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


@pytest.mark.asyncio
async def test_otel_shaped_ids_route_whole_traces(tmp_path: Path) -> None:
    """Under OTel ingestion, span_id never equals root_span_id.

    Routing must still group by root_span_id (the trace id), pulling both
    top-level matches in the trace and their nested children.
    """
    inserted, _, _ = await _run_migration(
        tmp_path / "inc",
        pages=[list(OTEL_SPANS)],
        config=MigrationConfig(logs_include_root_span_name=ROOT_NAME),
    )
    assert set(inserted) == {"o1", "o2", "o3", "o4"}

    excluded, _, _ = await _run_migration(
        tmp_path / "exc",
        pages=[list(OTEL_SPANS)],
        config=MigrationConfig(logs_exclude_root_span_name=ROOT_NAME),
    )
    assert set(excluded) == {"o5", "o6"}


@pytest.mark.asyncio
async def test_top_level_detection_uses_span_parents_not_id_equality(
    tmp_path: Path,
) -> None:
    """Regression: counting roots via span_id == root_span_id reported 0 on
    OTel data, which read as "the name is never top-level" when it always was."""
    from braintrust_migrate.btql import collect_root_span_ids_for_span_name

    client = _StubClient(
        pages=[list(OTEL_SPANS)],
        migration_config=MigrationConfig(),
    )
    _, stats = await collect_root_span_ids_for_span_name(
        client=client,  # type: ignore[arg-type]
        from_expr="project_logs('p') spans",
        span_name=ROOT_NAME,
        log_fields={},
    )

    # Both matches are top-level despite span_id != root_span_id, and both live
    # in the same trace, so one trace is routed by two matches.
    assert stats == {"matched_spans": 2, "root_spans": 2, "distinct_traces": 1}

    # A nested match must NOT be counted as top-level, which is what makes the
    # warning about over-broad selection meaningful.
    nested = [
        *OTEL_SPANS,
        _span(
            "o7",
            "aaaa1111bbbb2222",
            OTEL_OTHER_TRACE,
            ROOT_NAME,
            "qk7",
            span_parents=["94b27b8d9f5d224f"],
        ),
    ]
    _, nested_stats = await collect_root_span_ids_for_span_name(
        client=_StubClient(  # type: ignore[arg-type]
            pages=[nested], migration_config=MigrationConfig()
        ),
        from_expr="project_logs('p') spans",
        span_name=ROOT_NAME,
        log_fields={},
    )
    assert nested_stats == {
        "matched_spans": 3,
        "root_spans": 2,
        "distinct_traces": 2,
    }


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
