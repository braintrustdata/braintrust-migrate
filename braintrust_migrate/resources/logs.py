"""Logs migrator for Braintrust migration tool.

`project_logs` can be extremely large, so logs migration is implemented as a
streaming copier via BTQL sorted pagination.
"""

from __future__ import annotations

import json as _json
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, cast

import httpx
import structlog

from braintrust_migrate.attachments import AttachmentCopier
from braintrust_migrate.batching import approx_events_insert_payload_bytes, approx_json_bytes
from braintrust_migrate.btql import (
    btql_quote,
    fetch_btql_sorted_page_with_retries,
    find_first_pagination_key_for_created_after,
)
from braintrust_migrate.client import BraintrustClient
from braintrust_migrate.resources.base import MigrationState, ResourceMigrator
from braintrust_migrate.sdk_logs import SDKProjectLogsWriter
from braintrust_migrate.streaming_utils import (
    SeenIdsDB,
    build_btql_sorted_page_query,
)

logger = structlog.get_logger(__name__)

# HTTP status codes
HTTP_STATUS_REQUEST_ENTITY_TOO_LARGE = 413


@dataclass(slots=True)
class _LogsStreamingState:
    """Checkpoint state for streaming logs migration (small + restartable)."""

    version: str | None = None
    cursor: str | None = None
    fetched_events: int = 0
    inserted_events: int = 0
    inserted_bytes: int = 0
    skipped_seen: int = 0
    failed_batches: int = 0
    attachments_copied: int = 0
    # When using BTQL-sorted fetch, we cannot use the opaque cursor. Instead we
    # page using a stable sort key (_pagination_key).
    btql_min_pagination_key: str | None = None
    btql_min_pagination_key_inclusive: bool = False
    btql_last_created: str | None = None
    query_source: str | None = None
    created_after: str | None = None
    created_before: str | None = None

    @classmethod
    def from_path(cls, path: Path) -> _LogsStreamingState:
        if not path.exists():
            return cls()
        with open(path) as f:
            data = _json.load(f)
        return cls(
            version=data.get("version"),
            cursor=data.get("cursor"),
            fetched_events=int(data.get("fetched_events", 0)),
            inserted_events=int(data.get("inserted_events", 0)),
            inserted_bytes=int(data.get("inserted_bytes", 0)),
            skipped_seen=int(data.get("skipped_seen", 0)),
            failed_batches=int(data.get("failed_batches", 0)),
            attachments_copied=int(data.get("attachments_copied", 0)),
            btql_min_pagination_key=data.get("btql_min_pagination_key"),
            btql_min_pagination_key_inclusive=bool(
                data.get("btql_min_pagination_key_inclusive", False)
            ),
            btql_last_created=data.get("btql_last_created"),
            query_source=data.get("query_source"),
            created_after=data.get("created_after"),
            created_before=data.get("created_before"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "cursor": self.cursor,
            "fetched_events": self.fetched_events,
            "inserted_events": self.inserted_events,
            "inserted_bytes": self.inserted_bytes,
            "skipped_seen": self.skipped_seen,
            "failed_batches": self.failed_batches,
            "attachments_copied": self.attachments_copied,
            "btql_min_pagination_key": self.btql_min_pagination_key,
            "btql_min_pagination_key_inclusive": self.btql_min_pagination_key_inclusive,
            "btql_last_created": self.btql_last_created,
            "query_source": self.query_source,
            "created_after": self.created_after,
            "created_before": self.created_before,
        }


class LogsMigrator(ResourceMigrator[dict[str, Any]]):
    """Streaming migrator for Braintrust project logs."""

    SDK_FLUSH_MAX_ROWS: ClassVar[int] = 5_000
    SDK_FLUSH_MAX_BYTES: ClassVar[int] = 25 * 1024 * 1024

    _INSERT_FIELDS: ClassVar[set[str]] = {
        "input",
        "output",
        "expected",
        "error",
        "scores",
        "metadata",
        "tags",
        "metrics",
        "context",
        "span_attributes",
        "id",
        "created",
        "origin",
        "_object_delete",
        "_is_merge",
        "_merge_paths",
        "_array_delete",
        "_parent_id",
        "span_id",
        "root_span_id",
        "span_parents",
    }

    def __init__(
        self,
        source_client: BraintrustClient,
        dest_client: BraintrustClient,
        checkpoint_dir: Path,
        batch_size: int = 100,
        *,
        page_limit: int = 50,
        insert_batch_size: int = 200,
        use_version_snapshot: bool = True,
        use_seen_db: bool = True,
        progress_hook: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        # We do not want ResourceMigrator's heavyweight state for logs.
        super().__init__(source_client, dest_client, checkpoint_dir, batch_size=1)
        self.page_limit = page_limit
        self.insert_batch_size = insert_batch_size
        self.use_version_snapshot = use_version_snapshot
        self.use_seen_db = use_seen_db
        self._progress_hook = progress_hook
        _ = batch_size  # intentionally ignored

        self._logger = logger.bind(migrator=self.__class__.__name__)
        self._sdk_logs_writer: SDKProjectLogsWriter | None = None
        self._sdk_flush_max_rows = int(self.SDK_FLUSH_MAX_ROWS)
        self._sdk_flush_max_bytes = int(self.SDK_FLUSH_MAX_BYTES)

        self._stream_state_path = self.checkpoint_dir / "logs_streaming_state.json"
        self._stream_state = _LogsStreamingState.from_path(self._stream_state_path)
        self._logger.info(
            "Loaded logs streaming state",
            stream_state_path=str(self._stream_state_path),
            btql_min_pagination_key=self._stream_state.btql_min_pagination_key,
            cursor=self._stream_state.cursor,
            fetched_events=self._stream_state.fetched_events,
            inserted_events=self._stream_state.inserted_events,
            inserted_bytes=self._stream_state.inserted_bytes,
        )

        self._seen_db_path = self.checkpoint_dir / "logs_seen.sqlite3"
        # Logs are always fetched via BTQL. We intentionally do not use
        # `/v1/project_logs/{id}/fetch` anymore.

        self._attachment_copier: AttachmentCopier | None = None
        mig_cfg = getattr(self.source_client, "migration_config", None)
        copy_enabled = getattr(mig_cfg, "copy_attachments", False) is True
        if copy_enabled:
            self._attachment_copier = AttachmentCopier(
                source_client=self.source_client,
                dest_client=self.dest_client,
                max_bytes=int(
                    getattr(mig_cfg, "attachment_max_bytes", 50 * 1024 * 1024)
                ),
            )

        # Byte-aware insert batching config (best-effort; falls back to count-only if missing).
        cfg = getattr(self.dest_client, "migration_config", None) or getattr(
            self.source_client, "migration_config", None
        )
        try:
            max_req = int(getattr(cfg, "insert_max_request_bytes", 6 * 1024 * 1024))
            headroom = float(getattr(cfg, "insert_request_headroom_ratio", 0.5))
            if headroom <= 0:
                raise ValueError("headroom must be > 0")
            self._insert_max_bytes: int | None = int(max_req * headroom)
        except Exception:
            self._insert_max_bytes = None

    @property
    def resource_name(self) -> str:
        return "Logs"

    # ---- ResourceMigrator plumbing (not used for logs streaming) ----
    def _load_state(self) -> MigrationState:  # type: ignore[override]
        return MigrationState()

    def _save_state(self) -> None:  # type: ignore[override]
        return

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        return []

    async def migrate_resource(self, resource: dict[str, Any]) -> str:
        raise NotImplementedError

    def _save_stream_state(self) -> None:
        with open(self._stream_state_path, "w") as f:
            _json.dump(self._stream_state.to_dict(), f, indent=2)

    @staticmethod
    def _max_xact_id(events: list[dict[str, Any]]) -> str | None:
        xacts: list[str] = []
        for e in events:
            x = e.get("_xact_id")
            if isinstance(x, str) and x:
                xacts.append(x)
        if not xacts:
            return None
        try:
            return str(max(int(x) for x in xacts))
        except Exception:
            return max(xacts)

    def _event_to_insert(
        self, event: dict[str, Any], source_project_id: str
    ) -> dict[str, Any]:
        out: dict[str, Any] = {k: event[k] for k in self._INSERT_FIELDS if k in event}

        if "id" in event:
            out["id"] = event["id"]

        if "origin" not in out or out.get("origin") is None:
            origin: dict[str, Any] = {
                "object_type": "project_logs",
                "object_id": source_project_id,
                "id": event.get("id"),
            }
            if event.get("_xact_id") is not None:
                origin["_xact_id"] = event.get("_xact_id")
            if event.get("created") is not None:
                origin["created"] = event.get("created")
            out["origin"] = origin

        return out

    async def _fetch_page(
        self,
        *,
        project_id: str,
        cursor: str | None,
        version: str | None,
        limit: int,
    ) -> dict[str, Any]:
        # Logs are always fetched via BTQL. Cursor/version are intentionally unused.
        _ = cursor
        _ = version
        return await self._fetch_page_btql_sorted(project_id=project_id, limit=limit)

    async def _fetch_page_btql_sorted(
        self,
        *,
        project_id: str,
        limit: int,
    ) -> dict[str, Any]:
        """Fetch one page via POST /btql using native BTQL syntax, sorted by _pagination_key.

        This exists to allow inserting logs in created-ascending order, which makes
        destination `_xact_id` (and thus UI default ordering by `_pagination_key`)
        align with created time.
        """
        # NOTE:
        # - /btql expects `query` as a BTQL string.
        # - BTQL cursor pagination DOES NOT work when a `sort` clause is specified.
        #   For sorted pagination, we must do offset-based pagination by filtering on
        #   the last sort key values from the previous page.

        # Use native BTQL syntax (select:/from:/filter:/sort:/limit:) for compatibility
        # with data planes that don't yet support SQL mode.
        last_pagination_key = self._stream_state.btql_min_pagination_key
        last_pagination_key_inclusive = bool(
            self._stream_state.btql_min_pagination_key_inclusive
        )
        created_after = self._stream_state.created_after
        created_before = self._stream_state.created_before

        from_expr = f"project_logs('{btql_quote(project_id)}') spans"

        def _query_text_for_limit(n: int) -> str:
            return build_btql_sorted_page_query(
                from_expr=from_expr,
                limit=n,
                last_pagination_key=last_pagination_key,
                last_pagination_key_inclusive=last_pagination_key_inclusive,
                created_after=created_after,
                created_before=created_before,
                select="*",
            )

        # IMPORTANT: Do NOT advance the persisted pagination key here.
        # We only commit pagination progress after successful inserts, so resume
        # cannot skip rows if we fail mid-page.
        return await fetch_btql_sorted_page_with_retries(
            client=self.source_client,
            query_for_limit=_query_text_for_limit,
            configured_limit=int(limit),
            operation="btql_project_logs_page",
            log_fields={"source_project_id": project_id},
            timeout_seconds=120.0,
        )

    async def _insert_events(
        self, *, project_id: str, events: list[dict[str, Any]]
    ) -> None:
        if self._sdk_logs_writer is None:
            self._sdk_logs_writer = SDKProjectLogsWriter(
                self.dest_client, project_id
            )
        await self.dest_client.with_retry(
            "insert_project_logs_events",
            lambda: self._sdk_logs_writer.write_rows(events),
        )

    @staticmethod
    def _extract_ids(events: list[dict[str, Any]]) -> list[str]:
        ids: list[str] = []
        for event in events:
            event_id = event.get("id")
            if isinstance(event_id, str) and event_id:
                ids.append(event_id)
        return ids

    @staticmethod
    def _sum_event_bytes(events: list[dict[str, Any]]) -> int:
        return sum(approx_json_bytes(event) for event in events)

    @staticmethod
    def _is_http_413(exc: Exception) -> bool:
        return (
            isinstance(exc, httpx.HTTPStatusError)
            and exc.response is not None
            and int(exc.response.status_code) == HTTP_STATUS_REQUEST_ENTITY_TOO_LARGE
        )

    @staticmethod
    def _approx_event_size_bytes(event: dict[str, Any]) -> int | None:
        try:
            return len(_json.dumps(event, separators=(",", ":"), ensure_ascii=False))
        except Exception:
            return None

    @staticmethod
    def _count_attachment_refs(event: dict[str, Any]) -> int:
        def _walk(v: Any) -> int:
            if isinstance(v, dict):
                if v.get("type") == "braintrust_attachment" and isinstance(
                    v.get("key"), str
                ):
                    return 1
                return sum(_walk(x) for x in v.values())
            if isinstance(v, list):
                return sum(_walk(x) for x in v)
            return 0

        return _walk(event)

    def _dump_oversize_event_summary(
        self,
        *,
        cursor: str | None,
        dest_project_id: str,
        event: dict[str, Any],
        error: Exception,
    ) -> None:
        event_id = event.get("id")
        safe_id = str(event_id) if isinstance(event_id, str) and event_id else "unknown"
        root_span_id = event.get("root_span_id")
        span_id = event.get("span_id")
        approx_size = self._approx_event_size_bytes(event)
        attachment_refs = self._count_attachment_refs(event)
        path = self.checkpoint_dir / f"oversize_project_logs_event_{safe_id}.json"
        summary = {
            "error": str(error),
            "cursor": cursor,
            "dest_project_id": dest_project_id,
            "event_id": event.get("id"),
            "root_span_id": root_span_id,
            "span_id": span_id,
            "created": event.get("created"),
            "approx_size_bytes": approx_size,
            "attachment_refs": attachment_refs,
            "top_level_keys": sorted(list(event.keys())),
        }
        try:
            with open(path, "w") as f:
                _json.dump(summary, f, indent=2)
            self._logger.error(
                "Oversize event isolated (413). This specific event cannot be inserted.",
                summary_path=str(path),
                event_id=safe_id,
                root_span_id=root_span_id,
                span_id=span_id,
                approx_size_bytes=approx_size,
                attachment_refs=attachment_refs,
                cursor=cursor,
            )
        except Exception:
            self._logger.error(
                "Oversize event isolated; failed to write summary",
                event_id=safe_id,
                root_span_id=root_span_id,
                span_id=span_id,
                cursor=cursor,
            )

    async def migrate_all(
        self, project_id: str | None = None, max_concurrent: int | None = None
    ) -> dict[str, Any]:
        _ = max_concurrent
        if not project_id:
            return {
                "resource_type": self.resource_name,
                "total": 0,
                "migrated": 0,
                "skipped": 0,
                "failed": 0,
                "errors": [{"error": "Project ID is required for logs migration"}],
            }
        if not self.dest_project_id:
            return {
                "resource_type": self.resource_name,
                "total": 0,
                "migrated": 0,
                "skipped": 0,
                "failed": 0,
                "errors": [
                    {"error": "Destination project ID is required for logs migration"}
                ],
            }

        source_project_id = project_id
        dest_project_id = self.dest_project_id

        self._logger.info(
            "Starting streaming logs migration",
            source_project_id=source_project_id,
            dest_project_id=dest_project_id,
            page_limit=self.page_limit,
            insert_batch_size=self.insert_batch_size,
            use_version_snapshot=self.use_version_snapshot,
            use_seen_db=self.use_seen_db,
        )

        seen_db = (
            SeenIdsDB(str(self.checkpoint_dir / "logs_seen.sqlite3"))
            if self.use_seen_db
            else None
        )
        errors: list[dict[str, Any]] = []

        try:
            # BTQL-based streaming does not use version snapshots.

            # Optional time filters for streaming queries (persisted for safe resume).
            mig_cfg = getattr(self.source_client, "migration_config", None)
            created_after_cfg = getattr(mig_cfg, "created_after", None)
            created_before_cfg = getattr(mig_cfg, "created_before", None)

            # Validate and persist created_after filter
            if (
                self._stream_state.created_after is not None
                or created_after_cfg is not None
            ):
                if self._stream_state.created_after is None:
                    if not isinstance(created_after_cfg, str) or not created_after_cfg:
                        raise TypeError(
                            "migration_config.created_after must be a non-empty string when set"
                        )
                    self._stream_state.created_after = created_after_cfg
                    self._stream_state.query_source = "btql_sorted_date_filter"
                    self._save_stream_state()
                elif created_after_cfg is None:
                    raise ValueError(
                        "Checkpoint includes a created_after filter but this run does not. "
                        "Re-run with the same --created-after value or start a fresh checkpoint."
                    )
                elif not isinstance(created_after_cfg, str) or not created_after_cfg:
                    raise TypeError(
                        "migration_config.created_after must be a non-empty string when set"
                    )
                elif self._stream_state.created_after != created_after_cfg:
                    raise ValueError(
                        f"created_after mismatch vs checkpoint: checkpoint={self._stream_state.created_after!r} "
                        f"run={created_after_cfg!r}. Re-run with the checkpoint value or start a fresh checkpoint."
                    )

            # Validate and persist created_before filter
            if (
                self._stream_state.created_before is not None
                or created_before_cfg is not None
            ):
                if self._stream_state.created_before is None:
                    if not isinstance(created_before_cfg, str) or not created_before_cfg:
                        raise TypeError(
                            "migration_config.created_before must be a non-empty string when set"
                        )
                    self._stream_state.created_before = created_before_cfg
                    self._stream_state.query_source = "btql_sorted_date_filter"
                    self._save_stream_state()
                elif created_before_cfg is None:
                    raise ValueError(
                        "Checkpoint includes a created_before filter but this run does not. "
                        "Re-run with the same --created-before value or start a fresh checkpoint."
                    )
                elif not isinstance(created_before_cfg, str) or not created_before_cfg:
                    raise TypeError(
                        "migration_config.created_before must be a non-empty string when set"
                    )
                elif self._stream_state.created_before != created_before_cfg:
                    raise ValueError(
                        f"created_before mismatch vs checkpoint: checkpoint={self._stream_state.created_before!r} "
                        f"run={created_before_cfg!r}. Re-run with the checkpoint value or start a fresh checkpoint."
                    )

            # If this is the first BTQL page and created_after is set, preflight the first
            # matching pagination key so we can start near the boundary.
            if (
                isinstance(self._stream_state.created_after, str)
                and self._stream_state.created_after
                and not self._stream_state.btql_min_pagination_key
            ):
                start_pk = await find_first_pagination_key_for_created_after(
                    client=self.source_client,
                    from_expr=(
                        f"project_logs('{btql_quote(source_project_id)}') spans"
                    ),
                    created_after=self._stream_state.created_after,
                    operation="btql_project_logs_created_after_start_pk",
                    log_fields={
                        "source_project_id": source_project_id,
                        "created_after": self._stream_state.created_after,
                        "created_before": self._stream_state.created_before,
                    },
                )
                if start_pk is None:
                    self._logger.info(
                        "No project logs found in date range; finishing",
                        source_project_id=source_project_id,
                        dest_project_id=dest_project_id,
                        created_after=self._stream_state.created_after,
                        created_before=self._stream_state.created_before,
                    )
                    self._save_stream_state()
                    return {
                        "resource_type": self.resource_name,
                        "total": self._stream_state.fetched_events,
                        "migrated": self._stream_state.inserted_events,
                        "skipped": self._stream_state.skipped_seen,
                        "failed": self._stream_state.failed_batches,
                        "errors": errors,
                        "streaming": True,
                        "checkpoint": str(self._stream_state_path),
                        "seen_db": str(self._seen_db_path)
                        if self.use_seen_db
                        else None,
                        "version": self._stream_state.version,
                        "resume_cursor": self._stream_state.cursor,
                    }
                self._stream_state.btql_min_pagination_key = start_pk
                self._stream_state.btql_min_pagination_key_inclusive = True
                self._stream_state.query_source = "btql_sorted_date_filter"
                self._save_stream_state()

            progress_hook = self._progress_hook
            current_page_num: int | None = None
            current_page_events: int | None = None
            active_last_pk = self._stream_state.btql_min_pagination_key
            active_last_pk_inclusive = bool(
                self._stream_state.btql_min_pagination_key_inclusive
            )

            pending_events: list[dict[str, Any]] = []
            pending_seen_ids: set[str] = set()
            pending_row_bytes = 0
            pending_fetched_events = 0
            pending_inserted_events = 0
            pending_inserted_bytes = 0
            pending_skipped_seen = 0
            pending_attachments_copied = 0
            pending_last_pk: str | None = None
            pending_last_created: str | None = None

            def _pk_cursor_prefix() -> str | None:
                pk = pending_last_pk or active_last_pk
                return (pk[:16] + "…") if isinstance(pk, str) else pk

            def _save_state() -> None:
                # Logs are BTQL-only; cursor is intentionally unused.
                self._stream_state.cursor = None
                self._save_stream_state()

            async def _on_single_413(event: dict[str, Any], err: Exception) -> None:
                self._dump_oversize_event_summary(
                    cursor=self._stream_state.btql_min_pagination_key,
                    dest_project_id=dest_project_id,
                    event=event,
                    error=err,
                )

            def _on_fetch(info: dict[str, Any]) -> None:
                nonlocal current_page_num, current_page_events
                current_page_num = cast(int, info.get("page_num"))
                current_page_events = cast(int, info.get("page_events"))
                if progress_hook is None:
                    return
                progress_hook(
                    {
                        "resource": "logs",
                        "phase": "fetch",
                        "source_project_id": source_project_id,
                        "dest_project_id": dest_project_id,
                        "page_num": current_page_num,
                        "page_events": current_page_events,
                        "configured_fetch_limit": self.page_limit,
                        "configured_insert_batch_size": self.insert_batch_size,
                        "fetched_total": self._stream_state.fetched_events
                        + pending_fetched_events,
                        "inserted_total": self._stream_state.inserted_events,
                        "inserted_bytes_total": self._stream_state.inserted_bytes,
                        "skipped_seen_total": self._stream_state.skipped_seen
                        + pending_skipped_seen,
                        "attachments_copied_total": self._stream_state.attachments_copied
                        + pending_attachments_copied,
                        "pending_buffered_rows": pending_inserted_events,
                        "pending_buffered_bytes": pending_row_bytes,
                        "cursor": _pk_cursor_prefix(),
                    }
                )

            def _on_insert(insert_info: dict[str, Any]) -> None:
                if progress_hook is None:
                    return
                progress_hook(
                    {
                        "resource": "logs",
                        "phase": "insert",
                        "source_project_id": source_project_id,
                        "dest_project_id": dest_project_id,
                        "page_num": current_page_num,
                        "page_events": current_page_events,
                        "configured_fetch_limit": self.page_limit,
                        "configured_insert_batch_size": self.insert_batch_size,
                        **insert_info,
                        "fetched_total": self._stream_state.fetched_events,
                        "inserted_total": self._stream_state.inserted_events,
                        "inserted_bytes_total": self._stream_state.inserted_bytes,
                        "skipped_seen_total": self._stream_state.skipped_seen,
                        "attachments_copied_total": self._stream_state.attachments_copied,
                        "pending_buffered_rows": 0,
                        "pending_buffered_bytes": 0,
                        "cursor": _pk_cursor_prefix(),
                    }
                )

            def _on_page(info: dict[str, Any]) -> None:
                if progress_hook is None:
                    return
                progress_hook(
                    {
                        "resource": "logs",
                        "phase": "page",
                        "source_project_id": source_project_id,
                        "dest_project_id": dest_project_id,
                        "page_num": info.get("page_num"),
                        "page_events": info.get("page_events"),
                        "configured_fetch_limit": self.page_limit,
                        "configured_insert_batch_size": self.insert_batch_size,
                        "fetched_total": self._stream_state.fetched_events
                        + pending_fetched_events,
                        "inserted_total": self._stream_state.inserted_events,
                        "inserted_bytes_total": self._stream_state.inserted_bytes,
                        "skipped_seen_total": self._stream_state.skipped_seen
                        + pending_skipped_seen,
                        "attachments_copied_total": self._stream_state.attachments_copied
                        + pending_attachments_copied,
                        "pending_buffered_rows": pending_inserted_events,
                        "pending_buffered_bytes": pending_row_bytes,
                        "cursor": _pk_cursor_prefix(),
                        "next_cursor": None,
                    }
                )

            def _on_done(_info: dict[str, Any]) -> None:
                if progress_hook is None:
                    return
                progress_hook(
                    {
                        "resource": "logs",
                        "phase": "done",
                        "source_project_id": source_project_id,
                        "dest_project_id": dest_project_id,
                        "fetched_total": self._stream_state.fetched_events,
                        "inserted_total": self._stream_state.inserted_events,
                        "inserted_bytes_total": self._stream_state.inserted_bytes,
                        "skipped_seen_total": self._stream_state.skipped_seen,
                        "attachments_copied_total": self._stream_state.attachments_copied,
                        "pending_buffered_rows": 0,
                        "pending_buffered_bytes": 0,
                        "cursor": None,
                        "next_cursor": None,
                    }
                )

            def _on_batch_error(info: dict[str, Any]) -> None:
                e = cast(Exception, info.get("error"))
                batch = cast(list[dict[str, Any]], info.get("batch") or [])
                page_num = cast(int | None, info.get("page_num"))
                self._stream_state.failed_batches += 1
                try:
                    self._save_stream_state()
                except Exception:
                    pass

                err_type = type(e).__name__
                err_repr = repr(e)
                status_code: int | None = None
                response_text_excerpt: str | None = None
                request_url: str | None = None
                request_method: str | None = None
                if isinstance(e, httpx.HTTPStatusError):
                    if e.response is not None:
                        try:
                            status_code = int(e.response.status_code)
                        except Exception:
                            status_code = None
                        try:
                            response_text_excerpt = e.response.text[:500]
                        except Exception:
                            response_text_excerpt = None
                    if e.request is not None:
                        request_url = str(e.request.url)
                        request_method = e.request.method
                elif isinstance(e, httpx.RequestError):
                    if e.request is not None:
                        request_url = str(e.request.url)
                        request_method = e.request.method

                batch_first_id = batch[0].get("id") if batch else None
                batch_last_id = batch[-1].get("id") if batch else None
                approx_payload_bytes: int | None = None
                try:
                    approx_payload_bytes = len(
                        _json.dumps(
                            {"events": batch},
                            separators=(",", ":"),
                            ensure_ascii=False,
                        )
                    )
                except Exception:
                    approx_payload_bytes = None

                err = {
                    "error": str(e),
                    "error_type": err_type,
                    "error_repr": err_repr,
                    "cursor": self._stream_state.btql_min_pagination_key,
                    "batch_size": len(batch),
                    "page_num": page_num,
                    "batch_first_id": batch_first_id,
                    "batch_last_id": batch_last_id,
                    "approx_payload_bytes": approx_payload_bytes,
                    "status_code": status_code,
                    "request_method": request_method,
                    "request_url": request_url,
                    "response_text_excerpt": response_text_excerpt,
                }
                errors.append(err)
                self._logger.error("Batch insert failed", **err)

            def _set_last_pk(pk: str | None) -> None:
                self._stream_state.btql_min_pagination_key = pk
                self._stream_state.btql_min_pagination_key_inclusive = False

            async def _flush_pending_events() -> None:
                nonlocal pending_events
                nonlocal pending_seen_ids
                nonlocal pending_row_bytes
                nonlocal pending_fetched_events
                nonlocal pending_inserted_events
                nonlocal pending_inserted_bytes
                nonlocal pending_skipped_seen
                nonlocal pending_attachments_copied
                nonlocal pending_last_pk
                nonlocal pending_last_created

                if (
                    pending_fetched_events == 0
                    and pending_inserted_events == 0
                    and pending_skipped_seen == 0
                    and pending_attachments_copied == 0
                    and pending_last_pk is None
                ):
                    return

                if pending_events:
                    batch = list(pending_events)
                    started = time.perf_counter()
                    try:
                        await self._insert_events(
                            project_id=dest_project_id, events=batch
                        )
                    except Exception as e:
                        _on_batch_error(
                            {
                                "page_num": current_page_num,
                                "batch": batch,
                                "error": e,
                            }
                        )
                        if len(batch) == 1 and self._is_http_413(e):
                            await _on_single_413(batch[0], e)
                        raise
                    if seen_db is not None and pending_seen_ids:
                        seen_db.mark_seen(list(pending_seen_ids))
                    self._stream_state.inserted_events += pending_inserted_events
                    self._stream_state.inserted_bytes += pending_inserted_bytes
                    _on_insert(
                        {
                            "inserted_last": pending_inserted_events,
                            "inserted_bytes_last": pending_inserted_bytes,
                            "insert_seconds": max(0.0, time.perf_counter() - started),
                            "flush_rows": pending_inserted_events,
                            "flush_buffer_bytes": pending_row_bytes,
                        }
                    )

                self._stream_state.fetched_events += pending_fetched_events
                self._stream_state.skipped_seen += pending_skipped_seen
                self._stream_state.attachments_copied += pending_attachments_copied
                self._stream_state.btql_last_created = pending_last_created
                if pending_last_pk is not None:
                    _set_last_pk(pending_last_pk)
                _save_state()

                pending_events = []
                pending_seen_ids = set()
                pending_row_bytes = 0
                pending_fetched_events = 0
                pending_inserted_events = 0
                pending_inserted_bytes = 0
                pending_skipped_seen = 0
                pending_attachments_copied = 0
                pending_last_pk = None
                pending_last_created = None

            page_num = 0
            while True:
                page_num += 1
                current_page_num = page_num

                from_expr = f"project_logs('{btql_quote(source_project_id)}') spans"

                def _query_text_for_limit(n: int) -> str:
                    return build_btql_sorted_page_query(
                        from_expr=from_expr,
                        limit=n,
                        last_pagination_key=active_last_pk,
                        last_pagination_key_inclusive=active_last_pk_inclusive,
                        created_after=self._stream_state.created_after,
                        created_before=self._stream_state.created_before,
                        select="*",
                    )

                page = await fetch_btql_sorted_page_with_retries(
                    client=self.source_client,
                    query_for_limit=_query_text_for_limit,
                    configured_limit=int(self.page_limit),
                    operation="btql_project_logs_page",
                    log_fields={"source_project_id": source_project_id},
                    timeout_seconds=120.0,
                )
                page_events = cast(list[dict[str, Any]], page.get("events") or [])
                page_last_pk = cast(str | None, page.get("btql_last_pagination_key"))
                current_page_events = len(page_events)

                if page_events:
                    _on_fetch(
                        {
                            "page_num": page_num,
                            "page_events": len(page_events),
                            "configured_fetch_limit": int(self.page_limit),
                        }
                    )

                if not page_events:
                    await _flush_pending_events()
                    _save_state()
                    _on_done({"page_num": page_num})
                    break

                pending_fetched_events += len(page_events)

                insert_events_list = [
                    self._event_to_insert(event, source_project_id)
                    for event in page_events
                ]

                if seen_db is not None:
                    all_ids = self._extract_ids(insert_events_list)
                    if all_ids:
                        unseen = set(seen_db.filter_unseen(all_ids))
                        skipped_seen = len(all_ids) - len(unseen)
                        if skipped_seen:
                            pending_skipped_seen += skipped_seen
                        insert_events_list = [
                            event
                            for event in insert_events_list
                            if event.get("id") in unseen
                        ]

                if pending_seen_ids:
                    deduped_events: list[dict[str, Any]] = []
                    pending_duplicates = 0
                    for event in insert_events_list:
                        event_id = event.get("id")
                        if isinstance(event_id, str) and event_id in pending_seen_ids:
                            pending_duplicates += 1
                            continue
                        deduped_events.append(event)
                    if pending_duplicates:
                        pending_skipped_seen += pending_duplicates
                    insert_events_list = deduped_events

                if self._attachment_copier is not None:
                    copied = 0
                    for event in insert_events_list:
                        copied += int(
                            await self._attachment_copier.rewrite_event_in_place(event)
                        )
                    pending_attachments_copied += copied

                if insert_events_list:
                    pending_events.extend(insert_events_list)
                    pending_seen_ids.update(self._extract_ids(insert_events_list))
                    pending_inserted_events += len(insert_events_list)
                    pending_inserted_bytes += approx_events_insert_payload_bytes(
                        insert_events_list
                    )
                    pending_row_bytes += self._sum_event_bytes(insert_events_list)

                pending_last_pk = page_last_pk
                last_created = page_events[-1].get("created")
                if isinstance(last_created, str):
                    pending_last_created = last_created
                active_last_pk = page_last_pk
                active_last_pk_inclusive = False

                if (
                    pending_inserted_events >= self._sdk_flush_max_rows
                    or pending_row_bytes >= self._sdk_flush_max_bytes
                ):
                    await _flush_pending_events()

                _on_page(
                    {
                        "page_num": page_num,
                        "page_events": len(page_events),
                    }
                )

            return {
                "resource_type": self.resource_name,
                "total": self._stream_state.fetched_events,
                "migrated": self._stream_state.inserted_events,
                "skipped": self._stream_state.skipped_seen,
                "failed": self._stream_state.failed_batches,
                "errors": errors,
                "streaming": True,
                "checkpoint": str(self._stream_state_path),
                "seen_db": str(self._seen_db_path) if self.use_seen_db else None,
                "version": self._stream_state.version,
                "resume_cursor": self._stream_state.cursor,
            }
        finally:
            if seen_db is not None:
                seen_db.close()

    def get_partial_results(self) -> dict[str, Any]:
        """Best-effort results snapshot for orchestration error handling."""
        return {
            "resource_type": self.resource_name,
            "total": self._stream_state.fetched_events,
            "migrated": self._stream_state.inserted_events,
            "skipped": self._stream_state.skipped_seen,
            "failed": self._stream_state.failed_batches,
            "errors": [],
            "streaming": True,
            "checkpoint": str(self._stream_state_path),
            "seen_db": str(self._seen_db_path) if self.use_seen_db else None,
            "version": self._stream_state.version,
            "resume_cursor": self._stream_state.cursor,
        }
