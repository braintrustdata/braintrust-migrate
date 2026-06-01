"""Read-only validation for migrated project logs."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, cast

import structlog

from braintrust_migrate.btql import btql_quote, fetch_btql_sorted_page_with_retries
from braintrust_migrate.client import BraintrustClient
from braintrust_migrate.streaming_utils import build_btql_sorted_page_query

logger = structlog.get_logger(__name__)

_ROOT_LOG_SELECT = (
    "id, root_span_id, _xact_id, created, _pagination_key, is_root, span_parents, origin"
)


@dataclass(frozen=True, slots=True)
class RootSpanXactKey:
    """Source transaction identity for a migrated root log span."""

    root_span_id: str
    xact_id: str


@dataclass(slots=True)
class LogsRootXactProjectResult:
    """Logs root-span validation result for one project."""

    project_name: str
    source_project_id: str
    dest_project_id: str | None
    checked: int = 0
    matched: int = 0
    missing: list[dict[str, Any]] | None = None
    duplicate_destination: list[dict[str, Any]] | None = None
    unverifiable_source: int = 0
    unverifiable_destination: int = 0
    skipped_reason: str | None = None

    @property
    def success(self) -> bool:
        return (
            not self.missing
            and not self.duplicate_destination
            and self.unverifiable_source == 0
            and self.unverifiable_destination == 0
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_name": self.project_name,
            "source_project_id": self.source_project_id,
            "dest_project_id": self.dest_project_id,
            "checked": self.checked,
            "matched": self.matched,
            "missing": self.missing or [],
            "duplicate_destination": self.duplicate_destination or [],
            "unverifiable_source": self.unverifiable_source,
            "unverifiable_destination": self.unverifiable_destination,
            "skipped_reason": self.skipped_reason,
            "success": self.success,
        }


@dataclass(slots=True)
class LogsRootXactValidationSummary:
    """Aggregate logs root-span validation result."""

    projects: list[LogsRootXactProjectResult]

    @property
    def checked(self) -> int:
        return sum(project.checked for project in self.projects)

    @property
    def matched(self) -> int:
        return sum(project.matched for project in self.projects)

    @property
    def missing_count(self) -> int:
        return sum(len(project.missing or []) for project in self.projects)

    @property
    def duplicate_destination_count(self) -> int:
        return sum(len(project.duplicate_destination or []) for project in self.projects)

    @property
    def unverifiable_source(self) -> int:
        return sum(project.unverifiable_source for project in self.projects)

    @property
    def unverifiable_destination(self) -> int:
        return sum(project.unverifiable_destination for project in self.projects)

    @property
    def skipped_projects(self) -> int:
        return sum(1 for project in self.projects if project.skipped_reason)

    @property
    def success(self) -> bool:
        return all(project.success for project in self.projects)

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource": "logs",
            "checked": self.checked,
            "matched": self.matched,
            "missing": self.missing_count,
            "duplicate_destination": self.duplicate_destination_count,
            "unverifiable_source": self.unverifiable_source,
            "unverifiable_destination": self.unverifiable_destination,
            "skipped_projects": self.skipped_projects,
            "success": self.success,
            "projects": [project.to_dict() for project in self.projects],
        }


async def validate_logs_root_xacts(
    *,
    source_client: BraintrustClient,
    dest_client: BraintrustClient,
    projects: list[dict[str, str]],
    page_limit: int,
    created_after: str | None = None,
    created_before: str | None = None,
) -> LogsRootXactValidationSummary:
    """Validate migrated root project-log spans by source `_xact_id`.

    Destination rows receive new transaction ids, so this validator compares
    source `_xact_id` to destination `origin._xact_id`.
    """

    results: list[LogsRootXactProjectResult] = []
    for project in projects:
        name = project.get("name") or project.get("source_id") or "unknown"
        source_project_id = project.get("source_id") or ""
        dest_project_id = project.get("dest_id") or ""

        if not dest_project_id:
            results.append(
                LogsRootXactProjectResult(
                    project_name=name,
                    source_project_id=source_project_id,
                    dest_project_id=None,
                    skipped_reason="No matching destination project found by name",
                )
            )
            continue

        results.append(
            await _validate_project_logs_root_xacts(
                source_client=source_client,
                dest_client=dest_client,
                project_name=name,
                source_project_id=source_project_id,
                dest_project_id=dest_project_id,
                page_limit=page_limit,
                created_after=created_after,
                created_before=created_before,
            )
        )

    return LogsRootXactValidationSummary(projects=results)


async def _validate_project_logs_root_xacts(
    *,
    source_client: BraintrustClient,
    dest_client: BraintrustClient,
    project_name: str,
    source_project_id: str,
    dest_project_id: str,
    page_limit: int,
    created_after: str | None,
    created_before: str | None,
) -> LogsRootXactProjectResult:
    source_rows = await _fetch_root_log_rows(
        client=source_client,
        project_id=source_project_id,
        page_limit=page_limit,
        created_after=created_after,
        created_before=created_before,
        operation="validate_source_project_logs_root_xacts",
        log_fields={"source_project_id": source_project_id},
    )
    dest_rows = await _fetch_root_log_rows(
        client=dest_client,
        project_id=dest_project_id,
        page_limit=page_limit,
        created_after=created_after,
        created_before=created_before,
        operation="validate_dest_project_logs_root_xacts",
        log_fields={
            "source_project_id": source_project_id,
            "dest_project_id": dest_project_id,
        },
    )

    source_keys: set[RootSpanXactKey] = set()
    source_details: dict[RootSpanXactKey, dict[str, Any]] = {}
    unverifiable_source = 0
    for row in source_rows:
        key = _source_key(row)
        if key is None:
            unverifiable_source += 1
            continue
        source_keys.add(key)
        source_details.setdefault(key, _row_summary(row))

    dest_index: dict[RootSpanXactKey, list[dict[str, Any]]] = defaultdict(list)
    dest_xacts_by_root: dict[str, set[str]] = defaultdict(set)
    unverifiable_destination = 0
    for row in dest_rows:
        origin = row.get("origin")
        if not _origin_matches_source_project(origin, source_project_id):
            continue
        key = _destination_key(row)
        if key is None:
            unverifiable_destination += 1
            continue
        dest_index[key].append(_row_summary(row))
        dest_xacts_by_root[key.root_span_id].add(key.xact_id)

    missing: list[dict[str, Any]] = []
    matched = 0
    for key in sorted(source_keys, key=lambda k: (k.root_span_id, k.xact_id)):
        dest_matches = dest_index.get(key) or []
        if dest_matches:
            matched += 1
            continue
        detail = dict(source_details.get(key) or {})
        detail["root_span_id"] = key.root_span_id
        detail["_xact_id"] = key.xact_id
        detail["destination_origin_xact_ids_for_root"] = sorted(
            dest_xacts_by_root.get(key.root_span_id, set())
        )
        missing.append(detail)

    duplicate_destination: list[dict[str, Any]] = []
    for key, rows in sorted(
        dest_index.items(), key=lambda item: (item[0].root_span_id, item[0].xact_id)
    ):
        if key not in source_keys or len(rows) <= 1:
            continue
        duplicate_destination.append(
            {
                "root_span_id": key.root_span_id,
                "_xact_id": key.xact_id,
                "count": len(rows),
                "rows": rows,
            }
        )

    return LogsRootXactProjectResult(
        project_name=project_name,
        source_project_id=source_project_id,
        dest_project_id=dest_project_id,
        checked=len(source_keys),
        matched=matched,
        missing=missing,
        duplicate_destination=duplicate_destination,
        unverifiable_source=unverifiable_source,
        unverifiable_destination=unverifiable_destination,
    )


async def _fetch_root_log_rows(
    *,
    client: BraintrustClient,
    project_id: str,
    page_limit: int,
    created_after: str | None,
    created_before: str | None,
    operation: str,
    log_fields: dict[str, Any],
) -> list[dict[str, Any]]:
    try:
        return await _fetch_root_log_rows_once(
            client=client,
            project_id=project_id,
            page_limit=page_limit,
            created_after=created_after,
            created_before=created_before,
            operation=operation,
            log_fields=log_fields,
            use_server_root_filter=True,
        )
    except Exception as e:
        logger.warning(
            "Root-span BTQL filter failed; retrying validation with client-side filtering",
            error=str(e),
            **log_fields,
        )
        return await _fetch_root_log_rows_once(
            client=client,
            project_id=project_id,
            page_limit=page_limit,
            created_after=created_after,
            created_before=created_before,
            operation=f"{operation}_client_filter",
            log_fields=log_fields,
            use_server_root_filter=False,
        )


async def _fetch_root_log_rows_once(
    *,
    client: BraintrustClient,
    project_id: str,
    page_limit: int,
    created_after: str | None,
    created_before: str | None,
    operation: str,
    log_fields: dict[str, Any],
    use_server_root_filter: bool,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    active_last_pk: str | None = None
    from_expr = f"project_logs('{btql_quote(project_id)}') spans"
    extra_conditions = ["is_root = true"] if use_server_root_filter else None

    while True:
        query_last_pk = active_last_pk

        def _query_text_for_limit(
            n: int, *, _query_last_pk: str | None = query_last_pk
        ) -> str:
            return build_btql_sorted_page_query(
                from_expr=from_expr,
                limit=n,
                last_pagination_key=_query_last_pk,
                created_after=created_after,
                created_before=created_before,
                extra_conditions=extra_conditions,
                select=_ROOT_LOG_SELECT,
            )

        page = await fetch_btql_sorted_page_with_retries(
            client=client,
            query_for_limit=_query_text_for_limit,
            configured_limit=int(page_limit),
            operation=operation,
            log_fields=log_fields,
            timeout_seconds=120.0,
        )
        page_rows = cast(list[dict[str, Any]], page.get("events") or [])
        if not page_rows:
            break

        rows.extend(row for row in page_rows if _is_root_row(row))

        page_last_pk = page.get("btql_last_pagination_key")
        if not isinstance(page_last_pk, str) or not page_last_pk:
            raise ValueError("BTQL validation page is missing _pagination_key")
        active_last_pk = page_last_pk

    return rows


def _source_key(row: dict[str, Any]) -> RootSpanXactKey | None:
    root_span_id = row.get("root_span_id")
    xact_id = row.get("_xact_id")
    if not isinstance(root_span_id, str) or not root_span_id:
        return None
    if not isinstance(xact_id, str) or not xact_id:
        return None
    return RootSpanXactKey(root_span_id=root_span_id, xact_id=xact_id)


def _destination_key(row: dict[str, Any]) -> RootSpanXactKey | None:
    root_span_id = row.get("root_span_id")
    origin = row.get("origin")
    xact_id = origin.get("_xact_id") if isinstance(origin, dict) else None
    if not isinstance(root_span_id, str) or not root_span_id:
        return None
    if not isinstance(xact_id, str) or not xact_id:
        return None
    return RootSpanXactKey(root_span_id=root_span_id, xact_id=xact_id)


def _origin_matches_source_project(origin: Any, source_project_id: str) -> bool:
    return (
        isinstance(origin, dict)
        and origin.get("object_type") == "project_logs"
        and origin.get("object_id") == source_project_id
    )


def _is_root_row(row: dict[str, Any]) -> bool:
    if row.get("is_root") is True:
        return True
    return "is_root" not in row and row.get("span_parents") == []


def _row_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "root_span_id": row.get("root_span_id"),
        "_xact_id": row.get("_xact_id"),
        "origin": row.get("origin"),
        "created": row.get("created"),
    }
