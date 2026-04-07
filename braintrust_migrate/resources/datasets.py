"""Dataset migrator for Braintrust migration tool."""

from __future__ import annotations

import json as _json
import hashlib
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar

import httpx
import structlog

from braintrust_migrate.attachments import AttachmentCopier
from braintrust_migrate.btql import (
    btql_quote,
    fetch_btql_sorted_page_with_retries,
)
from braintrust_migrate.resources.base import MigrationResult, ResourceMigrator
from braintrust_migrate.sdk_logs import SDKDatasetWriter
from braintrust_migrate.streaming_utils import (
    EventsStreamState,
    SeenIdsDB,
    build_btql_sorted_page_query,
    stream_btql_sorted_events_buffered,
)

logger = structlog.get_logger(__name__)

# HTTP status codes
HTTP_STATUS_REQUEST_ENTITY_TOO_LARGE = 413


def _coerce_int_config(
    cfg: Any, attr_name: str, default: int, *, minimum: int | None = None
) -> int:
    value = getattr(cfg, attr_name, default)
    if not isinstance(value, int):
        try:
            value = int(value)
        except Exception:
            value = default
    if minimum is not None and value < minimum:
        return default
    return value


class DatasetMigrator(ResourceMigrator[dict]):
    """Migrator for Braintrust datasets.

    Handles migration of:
    - Dataset metadata (name, description, etc.)
    - Dataset records/items
    - Brainstore blobs if enabled
    - Uses bulk operations for better performance

    Uses raw API requests instead of SDK to avoid model dependencies.
    """

    SDK_FLUSH_MAX_ROWS: ClassVar[int] = 5_000
    SDK_FLUSH_MAX_BYTES: ClassVar[int] = 25 * 1024 * 1024
    DEFAULT_EVENT_FETCH_GROUP_SIZE: ClassVar[int] = 25

    def __init__(
        self,
        source_client,
        dest_client,
        checkpoint_dir: Path,
        batch_size: int = 100,
        *,
        events_fetch_limit: int = 1000,
        events_use_seen_db: bool = True,
        events_progress_hook: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        super().__init__(
            source_client, dest_client, checkpoint_dir, batch_size=batch_size
        )
        self.events_fetch_limit = events_fetch_limit
        self.events_use_seen_db = events_use_seen_db
        self._events_progress_hook = events_progress_hook
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
        self._sdk_flush_max_rows = int(self.SDK_FLUSH_MAX_ROWS)
        self._sdk_flush_max_bytes = int(self.SDK_FLUSH_MAX_BYTES)
        self._event_fetch_group_size = _coerce_int_config(
            cfg,
            "events_fetch_group_size",
            self.DEFAULT_EVENT_FETCH_GROUP_SIZE,
            minimum=1,
        )

    @property
    def resource_name(self) -> str:
        """Human-readable name for this resource type."""
        return "Datasets"

    @property
    def allowed_fields_for_event_insert(self) -> set[str] | None:
        """Fields that are allowed when inserting dataset events.

        Uses the InsertDatasetEvent schema from OpenAPI spec.

        Returns:
            Set of field names allowed for insertion, or None if schema not found.
        """
        from braintrust_migrate.openapi_utils import get_resource_create_fields

        return get_resource_create_fields("DatasetEvent")

    # FALLBACK: Static field list for InsertDatasetEvent
    # This is only used if OpenAPI schema is unavailable
    # Primary source: allowed_fields_for_event_insert property (uses OpenAPI spec)
    _EVENT_INSERT_FIELDS: ClassVar[set[str]] = {
        "input",
        "expected",
        "metadata",
        "tags",
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

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        """List all datasets from the source organization using raw API.

        Args:
            project_id: Optional project ID to filter datasets.

        Returns:
            List of dataset dicts from the source organization.
        """
        self._logger.info("Listing datasets from source organization")

        try:
            # Use base class helper method (now uses raw_request)
            return await self._list_resources_with_client(
                self.source_client, "datasets", project_id
            )

        except Exception as e:
            self._logger.error("Failed to list source datasets", error=str(e))
            raise

    async def migrate_batch(self, resources: list[dict]) -> list[MigrationResult]:
        """Migrate a batch of datasets using bulk operations for better performance.

        This overrides the base migrate_batch to:
        1. Create all datasets in the batch
        2. Migrate records for all successfully created datasets

        Args:
            resources: List of datasets to migrate.

        Returns:
            List of migration results.
        """
        if not resources:
            return []

        results = []
        datasets_to_create = []

        # First pass: check for already migrated datasets
        for resource in resources:
            source_id = self.get_resource_id(resource)

            # Check if already migrated
            if source_id in self.state.id_mapping:
                dest_id = self.state.id_mapping[source_id]
                resource_name = getattr(resource, "name", None)

                self._logger.info(
                    "⏭️  Skipped dataset (already migrated)",
                    source_id=source_id,
                    dest_id=dest_id,
                    name=resource_name,
                )

                results.append(
                    MigrationResult(
                        success=True,
                        source_id=source_id,
                        dest_id=dest_id,
                        skipped=True,
                        metadata={
                            "name": resource_name,
                            "skip_reason": "already_migrated",
                        }
                        if resource_name
                        else {"skip_reason": "already_migrated"},
                    )
                )
                continue

            # Prepare for creation
            datasets_to_create.append(resource)

        if not datasets_to_create:
            return results

        # Phase 1: Create all datasets
        self._logger.info(
            "Creating datasets in batch",
            count=len(datasets_to_create),
        )

        dataset_creation_results = await self._create_datasets_batch(datasets_to_create)
        results.extend(dataset_creation_results)

        # Phase 2: Migrate records for all successfully created datasets
        successful_migrations = [
            r for r in dataset_creation_results if r.success and not r.skipped
        ]
        if successful_migrations:
            await self._migrate_records_for_datasets(successful_migrations)

        return results

    async def _create_datasets_batch(
        self, datasets: list[dict]
    ) -> list[MigrationResult]:
        """Create a batch of datasets using raw API.

        Args:
            datasets: List of dataset dicts to create

        Returns:
            List of migration results for dataset creation
        """
        results = []

        for dataset in datasets:
            source_id = self.get_resource_id(dataset)

            try:
                # Create dataset in destination using OpenAPI allow-list filtering
                create_params = self.serialize_resource_for_insert(dataset)
                create_params["project_id"] = self.dest_project_id

                # Create dataset using raw API
                response = await self.dest_client.with_retry(
                    "create_dataset",
                    lambda create_params=create_params: self.dest_client.raw_request(
                        "POST",
                        "/v1/dataset",
                        json=create_params,
                    ),
                )

                dest_dataset_id = response.get("id")
                if not dest_dataset_id:
                    raise ValueError(
                        f"No ID returned when creating dataset {dataset.get('name')}"
                    )

                self._logger.info(
                    "✅ Created dataset",
                    source_id=source_id,
                    dest_id=dest_dataset_id,
                    name=dataset.get("name"),
                )

                # Record success immediately for any potential dependencies
                self.record_success(source_id, dest_dataset_id, dataset)

                results.append(
                    MigrationResult(
                        success=True,
                        source_id=source_id,
                        dest_id=dest_dataset_id,
                        metadata={"name": dataset.get("name"), "records_pending": True},
                    )
                )

            except Exception as e:
                error_msg = f"Failed to create dataset: {e}"
                self._logger.error(
                    error_msg,
                    source_id=source_id,
                    name=dataset.get("name"),
                )

                self.record_failure(source_id, error_msg)
                results.append(
                    MigrationResult(
                        success=False,
                        source_id=source_id,
                        error=error_msg,
                    )
                )

        return results

    async def _migrate_records_for_datasets(
        self, successful_migrations: list[MigrationResult]
    ) -> None:
        """Migrate records for all successfully created datasets.

        Args:
            successful_migrations: List of successful dataset migration results
        """
        self._logger.info(
            "Starting bulk record migration",
            dataset_count=len(successful_migrations),
        )

        async def _flush_group(group: list[MigrationResult]) -> None:
            if not group:
                return
            source_to_dest: dict[str, str] = {}
            for result in group:
                if result.dest_id is None:
                    raise ValueError("Dataset migrated without dest_id; cannot copy records")
                source_to_dest[result.source_id] = result.dest_id

            await self._migrate_dataset_records_streaming_grouped(source_to_dest)

            for result in group:
                if result.metadata:
                    result.metadata["records_pending"] = False
                    result.metadata["records_migrated"] = True

        for start in range(0, len(successful_migrations), self._event_fetch_group_size):
            group = successful_migrations[start : start + self._event_fetch_group_size]
            try:
                await _flush_group(group)
            except Exception as e:
                self._logger.error(
                    "Failed to migrate records for dataset group",
                    source_ids=[result.source_id for result in group],
                    error=str(e),
                )
                for result in group:
                    if result.metadata:
                        result.metadata["records_pending"] = False
                        result.metadata["records_failed"] = True

        self._logger.info(
            "Completed bulk record migration",
            dataset_count=len(successful_migrations),
        )

    async def migrate_resource(self, resource: dict) -> str:
        """Migrate a single dataset from source to destination using raw API.

        Note: This method is kept for compatibility but migrate_batch should be used
        for better performance when migrating multiple datasets.

        Args:
            resource: Source dataset dict to migrate.

        Returns:
            ID of the created dataset in destination.

        Raises:
            Exception: If migration fails.
        """
        self._logger.info(
            "Migrating dataset",
            source_id=resource.get("id"),
            name=resource.get("name"),
            project_id=resource.get("project_id"),
        )

        # Create dataset in destination using OpenAPI allow-list filtering
        create_params = self.serialize_resource_for_insert(resource)
        create_params["project_id"] = self.dest_project_id

        # Create dataset using raw API
        response = await self.dest_client.with_retry(
            "create_dataset",
            lambda create_params=create_params: self.dest_client.raw_request(
                "POST",
                "/v1/dataset",
                json=create_params,
            ),
        )

        dest_dataset_id = response.get("id")
        if not dest_dataset_id:
            raise ValueError(
                f"No ID returned when creating dataset {resource.get('name')}"
            )

        self._logger.info(
            "Created dataset in destination",
            source_id=resource.get("id"),
            dest_id=dest_dataset_id,
            name=resource.get("name"),
        )

        # Migrate dataset records/items
        await self._migrate_dataset_records(resource["id"], dest_dataset_id)

        return dest_dataset_id

    async def _migrate_dataset_records(
        self, source_dataset_id: str, dest_dataset_id: str
    ) -> None:
        """Migrate records from source dataset to destination dataset.

        Args:
            source_dataset_id: Source dataset ID.
            dest_dataset_id: Destination dataset ID.
        """
        self._logger.info(
            "Migrating dataset records",
            source_dataset_id=source_dataset_id,
            dest_dataset_id=dest_dataset_id,
        )

        await self._migrate_dataset_records_streaming(
            source_dataset_id, dest_dataset_id
        )

    # ---- Streaming dataset event copier (scales to large datasets) ----

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
        self, event: dict[str, Any], source_dataset_id: str
    ) -> dict[str, Any]:
        # Use dynamic schema from OpenAPI instead of static list
        allowed_fields = self.allowed_fields_for_event_insert
        if allowed_fields:
            out: dict[str, Any] = {k: event[k] for k in allowed_fields if k in event}
        else:
            # Fallback to static list if OpenAPI schema not available
            out = {k: event[k] for k in self._EVENT_INSERT_FIELDS if k in event}

        if "id" in event:
            out["id"] = event["id"]
        if "origin" not in out or out.get("origin") is None:
            origin: dict[str, Any] = {
                "object_type": "dataset",
                "object_id": source_dataset_id,
                "id": event.get("id"),
            }
            if event.get("_xact_id") is not None:
                origin["_xact_id"] = event.get("_xact_id")
            if event.get("created") is not None:
                origin["created"] = event.get("created")
            out["origin"] = origin
        out["dataset_id"] = source_dataset_id
        return out

    def _event_to_insert_from_row(self, event: dict[str, Any]) -> dict[str, Any]:
        source_dataset_id = event.get("dataset_id")
        if not isinstance(source_dataset_id, str) or not source_dataset_id:
            raise ValueError("Fetched dataset event missing dataset_id")
        return self._event_to_insert(event, source_dataset_id)

    async def _fetch_dataset_events_page(
        self,
        *,
        dataset_id: str,
        cursor: str | None,
        version: str | None,
        limit: int,
        state: EventsStreamState,
    ) -> dict[str, Any]:
        # Datasets are fetched via BTQL. Cursor/version are intentionally unused.
        _ = cursor
        _ = version
        return await self._fetch_dataset_events_page_btql_sorted(
            dataset_ids=[dataset_id], limit=limit, state=state
        )

    async def _fetch_dataset_events_page_btql_sorted(
        self,
        *,
        dataset_ids: list[str],
        limit: int,
        state: EventsStreamState,
    ) -> dict[str, Any]:
        """Fetch one page via POST /btql using native BTQL syntax, sorted by _pagination_key."""
        last_pagination_key = state.btql_min_pagination_key
        quoted_ids = ", ".join(f"'{btql_quote(dataset_id)}'" for dataset_id in dataset_ids)

        def _query_text_for_limit(n: int) -> str:
            return build_btql_sorted_page_query(
                from_expr=f"dataset({quoted_ids}) spans",
                limit=n,
                last_pagination_key=last_pagination_key,
                select="*",
            )

        return await fetch_btql_sorted_page_with_retries(
            client=self.source_client,
            query_for_limit=_query_text_for_limit,
            configured_limit=int(limit),
            operation="btql_dataset_events_page",
            log_fields={"source_dataset_ids": dataset_ids},
            timeout_seconds=120.0,
        )

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
        events_dir: Path,
        cursor: str | None,
        dest_dataset_id: str,
        event: dict[str, Any],
        error: Exception,
    ) -> None:
        event_id = event.get("id")
        safe_id = str(event_id) if isinstance(event_id, str) and event_id else "unknown"
        root_span_id = event.get("root_span_id")
        span_id = event.get("span_id")
        approx_size = self._approx_event_size_bytes(event)
        attachment_refs = self._count_attachment_refs(event)
        path = events_dir / f"oversize_dataset_event_{safe_id}.json"
        summary = {
            "error": str(error),
            "cursor": cursor,
            "dest_dataset_id": dest_dataset_id,
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
                "Oversize dataset event isolated (413). This specific event cannot be inserted.",
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
                "Oversize dataset event isolated; failed to write summary",
                event_id=safe_id,
                root_span_id=root_span_id,
                span_id=span_id,
                cursor=cursor,
            )

    @staticmethod
    def _group_stream_basename(source_dataset_ids: list[str]) -> str:
        if len(source_dataset_ids) == 1:
            return source_dataset_ids[0]
        joined = ",".join(source_dataset_ids)
        digest = hashlib.sha1(joined.encode("utf-8")).hexdigest()[:12]
        return f"group_{digest}"

    async def _insert_dataset_events_grouped(
        self,
        *,
        batch: list[dict[str, Any]],
        writers_by_source: dict[str, SDKDatasetWriter],
    ) -> None:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for event in batch:
            source_dataset_id = event.get("dataset_id")
            if not isinstance(source_dataset_id, str) or not source_dataset_id:
                raise ValueError("Dataset event batch missing source dataset_id")
            grouped.setdefault(source_dataset_id, []).append(event)

        for source_dataset_id, rows in grouped.items():
            writer = writers_by_source.get(source_dataset_id)
            if writer is None:
                raise KeyError(f"No destination writer for source dataset {source_dataset_id}")
            await self.dest_client.with_retry(
                "insert_dataset_events",
                lambda rows=rows, writer=writer: writer.write_rows(rows),
            )

    async def _migrate_dataset_records_streaming(
        self, source_dataset_id: str, dest_dataset_id: str
    ) -> None:
        await self._migrate_dataset_records_streaming_grouped(
            {source_dataset_id: dest_dataset_id}
        )

    async def _migrate_dataset_records_streaming_grouped(
        self, source_to_dest_dataset_ids: dict[str, str]
    ) -> None:
        events_dir = self.checkpoint_dir / "dataset_events"
        events_dir.mkdir(parents=True, exist_ok=True)
        source_dataset_ids = list(source_to_dest_dataset_ids.keys())
        group_basename = self._group_stream_basename(source_dataset_ids)

        state_path = events_dir / f"{group_basename}_state.json"
        state = EventsStreamState.from_path(state_path)

        seen_db = (
            SeenIdsDB(str(events_dir / f"{group_basename}_seen.sqlite3"))
            if self.events_use_seen_db
            else None
        )

        try:
            if state.btql_min_pagination_key is None and state.cursor is not None:
                # Legacy checkpoint format stored an opaque cursor (from /fetch). We
                # cannot translate this to a BTQL pagination key. Start from the
                # beginning and rely on seen_db for idempotency.
                self._logger.warning(
                    "Dataset events checkpoint contains legacy cursor but no btql_min_pagination_key; restarting BTQL stream from beginning",
                    source_dataset_ids=source_dataset_ids,
                )
                state.cursor = None

            writers_by_source = {
                source_dataset_id: SDKDatasetWriter(self.dest_client, dest_dataset_id)
                for source_dataset_id, dest_dataset_id in source_to_dest_dataset_ids.items()
            }

            progress = self._events_progress_hook

            def _save_state() -> None:
                state.cursor = None
                with open(state_path, "w") as f:
                    _json.dump(state.to_dict(), f, indent=2)

            async def _fetch(n: int) -> dict[str, Any]:
                return await self._fetch_dataset_events_page(
                    dataset_id=source_dataset_ids[0],
                    cursor=None,
                    version=None,
                    limit=n,
                    state=state,
                )

            async def _on_single_413(event: dict[str, Any], err: Exception) -> None:
                self._dump_oversize_event_summary(
                    events_dir=events_dir,
                    cursor=state.btql_min_pagination_key,
                    dest_dataset_id=(
                        source_to_dest_dataset_ids.get(source_dataset_id)
                        if isinstance(source_dataset_id, str)
                        else None
                    )
                    or "unknown",
                    event=event,
                    error=err,
                )

            async def _fetch_group(n: int) -> dict[str, Any]:
                return await self._fetch_dataset_events_page_btql_sorted(
                    dataset_ids=source_dataset_ids,
                    limit=n,
                    state=state,
                )

            event_to_insert = (
                self._event_to_insert_from_row
                if len(source_dataset_ids) > 1
                else lambda event, _source_dataset_id=source_dataset_ids[0]: self._event_to_insert(
                    event, _source_dataset_id
                )
            )

            await stream_btql_sorted_events_buffered(
                fetch_page=_fetch_group,
                page_limit=int(self.events_fetch_limit),
                state=state,
                save_state=_save_state,
                page_event_filter=lambda e: e.get("_object_delete") is True,
                event_to_insert=event_to_insert,
                seen_db=seen_db,
                rewrite_event_in_place=(
                    None
                    if self._attachment_copier is None
                    else self._attachment_copier.rewrite_event_in_place
                ),
                insert_events=lambda batch: self._insert_dataset_events_grouped(
                    batch=batch,
                    writers_by_source=writers_by_source,
                ),
                flush_max_rows=self._sdk_flush_max_rows,
                flush_max_bytes=self._sdk_flush_max_bytes,
                is_http_413=self._is_http_413,
                on_single_413=_on_single_413,
                hooks=None
                if progress is None
                else {
                    "on_fetch": lambda info, _p=progress: _p(
                        {
                            "resource": "dataset_events",
                            "phase": "fetch",
                            "source_dataset_ids": source_dataset_ids,
                            "dest_dataset_ids": list(source_to_dest_dataset_ids.values()),
                            "page_num": info.get("page_num"),
                            "page_events": info.get("page_events"),
                            "fetched_total": info.get("fetched_total"),
                            "inserted_total": info.get("inserted_total"),
                            "inserted_bytes_total": info.get("inserted_bytes_total"),
                            "skipped_deleted_total": info.get("skipped_deleted_total"),
                            "skipped_seen_total": info.get("skipped_seen_total"),
                            "attachments_copied_total": info.get("attachments_copied_total"),
                            "pending_buffered_rows": info.get("pending_buffered_rows"),
                            "pending_buffered_bytes": info.get("pending_buffered_bytes"),
                            "cursor": (
                                (state.btql_min_pagination_key[:16] + "…")
                                if isinstance(state.btql_min_pagination_key, str)
                                else None
                            ),
                            "next_cursor": None,
                        }
                    ),
                    "on_page": lambda info, _p=progress: _p(
                        {
                            "resource": "dataset_events",
                            "phase": "page",
                            "source_dataset_ids": source_dataset_ids,
                            "dest_dataset_ids": list(source_to_dest_dataset_ids.values()),
                            "page_num": info.get("page_num"),
                            "page_events": info.get("page_events"),
                            "fetched_total": info.get("fetched_total"),
                            "inserted_total": info.get("inserted_total"),
                            "inserted_bytes_total": info.get("inserted_bytes_total"),
                            "skipped_deleted_total": info.get("skipped_deleted_total"),
                            "skipped_seen_total": info.get("skipped_seen_total"),
                            "attachments_copied_total": info.get("attachments_copied_total"),
                            "pending_buffered_rows": info.get("pending_buffered_rows"),
                            "pending_buffered_bytes": info.get("pending_buffered_bytes"),
                            "cursor": (
                                (state.btql_min_pagination_key[:16] + "…")
                                if isinstance(state.btql_min_pagination_key, str)
                                else None
                            ),
                            "next_cursor": None,
                        }
                    ),
                    "on_done": lambda info, _p=progress: _p(
                        {
                            "resource": "dataset_events",
                            "phase": "done",
                            "source_dataset_ids": source_dataset_ids,
                            "dest_dataset_ids": list(source_to_dest_dataset_ids.values()),
                            "fetched_total": info.get("fetched_total"),
                            "inserted_total": info.get("inserted_total"),
                            "inserted_bytes_total": info.get("inserted_bytes_total"),
                            "skipped_deleted_total": info.get("skipped_deleted_total"),
                            "skipped_seen_total": info.get("skipped_seen_total"),
                            "attachments_copied_total": info.get("attachments_copied_total"),
                            "pending_buffered_rows": info.get("pending_buffered_rows"),
                            "pending_buffered_bytes": info.get("pending_buffered_bytes"),
                            "cursor": None,
                            "next_cursor": None,
                        }
                    ),
                },
            )

            self._logger.info(
                "Migrated dataset records (streaming)",
                source_dataset_ids=source_dataset_ids,
                dest_dataset_ids=list(source_to_dest_dataset_ids.values()),
                fetched=state.fetched_events,
                inserted=state.inserted_events,
                skipped_deleted=state.skipped_deleted,
                skipped_seen=state.skipped_seen,
            )
        finally:
            if seen_db is not None:
                seen_db.close()
