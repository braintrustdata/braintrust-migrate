"""Experiment migrator for Braintrust migration tool."""

from __future__ import annotations

import json as _json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, cast

import httpx
from braintrust_api.types import Experiment

from braintrust_migrate.attachments import AttachmentCopier
from braintrust_migrate.btql import (
    btql_quote,
    fetch_btql_sorted_page_with_retries,
)
from braintrust_migrate.resources.base import MigrationResult, ResourceMigrator
from braintrust_migrate.streaming_utils import (
    SeenIdsDB,
    build_btql_sorted_page_query,
    stream_btql_sorted_events,
)

# HTTP status codes
HTTP_STATUS_REQUEST_ENTITY_TOO_LARGE = 413


class ExperimentMigrator(ResourceMigrator[Experiment]):
    """Migrator for Braintrust experiments.

    Features:
    - Migrates experiment metadata and settings
    - Migrates all experiment events (evaluations) for each experiment
    - Handles experiment dependencies (base_exp_id, dataset_id)
    - Uses bulk operations for better performance
    """

    def __init__(
        self,
        source_client,
        dest_client,
        checkpoint_dir: Path,
        batch_size: int = 100,
        *,
        events_fetch_limit: int = 50,
        events_insert_batch_size: int = 200,
        events_use_version_snapshot: bool = True,
        events_use_seen_db: bool = True,
        events_progress_hook: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        super().__init__(
            source_client, dest_client, checkpoint_dir, batch_size=batch_size
        )
        self.events_fetch_limit = events_fetch_limit
        self.events_insert_batch_size = events_insert_batch_size
        self.events_use_version_snapshot = events_use_version_snapshot
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

    @property
    def resource_name(self) -> str:
        """Human-readable name for this resource type."""
        return "Experiments"

    @property
    def allowed_fields_for_event_insert(self) -> set[str] | None:
        """Fields that are allowed when inserting experiment events.

        Uses the InsertExperimentEvent schema from OpenAPI spec.

        Returns:
            Set of field names allowed for insertion, or None if schema not found.
        """
        from braintrust_migrate.openapi_utils import get_resource_create_fields

        return get_resource_create_fields("ExperimentEvent")

    # InsertExperimentEvent allow-list (based on docs/OpenAPI)
    _EVENT_INSERT_FIELDS: ClassVar[set[str]] = {
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

    def _sort_experiments_by_dependencies(
        self, experiments: list[Experiment]
    ) -> list[Experiment]:
        """Sort experiments using a simple two-pass approach.

        Pass 1: Experiments without base_exp_id (no dependencies)
        Pass 2: Experiments with base_exp_id (dependent experiments)

        Args:
            experiments: List of experiments to sort

        Returns:
            List of experiments sorted by dependency order
        """
        independent_experiments = []
        dependent_experiments = []

        for exp in experiments:
            if hasattr(exp, "base_exp_id") and exp.base_exp_id:
                dependent_experiments.append(exp)
            else:
                independent_experiments.append(exp)

        self._logger.info(
            "Split experiments by dependencies",
            independent=len(independent_experiments),
            dependent=len(dependent_experiments),
        )

        return independent_experiments + dependent_experiments

    async def list_source_resources(
        self, project_id: str | None = None
    ) -> list[Experiment]:
        """List all experiments from the source organization, sorted by dependencies."""
        try:
            experiments = await self._list_resources_with_client(
                self.source_client, "experiments", project_id
            )

            if experiments:
                # Sort experiments by dependencies for proper migration order
                experiments = self._sort_experiments_by_dependencies(experiments)

                self._logger.info(
                    "Found experiments for migration",
                    total=len(experiments),
                    project_id=project_id,
                )

            return experiments

        except Exception as e:
            self._logger.error("Failed to list source experiments", error=str(e))
            raise

    async def get_dependencies(self, resource: Experiment) -> list[str]:
        """Get list of resource IDs that this experiment depends on."""
        dependencies = []

        if hasattr(resource, "dataset_id") and resource.dataset_id:
            dependencies.append(resource.dataset_id)

        if hasattr(resource, "base_exp_id") and resource.base_exp_id:
            dependencies.append(resource.base_exp_id)

        return dependencies

    async def get_dependency_types(self) -> list[str]:
        """Get list of resource types that experiments might depend on."""
        return ["datasets", "experiments"]

    async def migrate_batch(self, resources: list[Experiment]) -> list[MigrationResult]:
        """Migrate a batch of experiments with dependency-aware ordering.

        This handles base_exp_id dependencies by:
        1. Creating experiments without base_exp_id first
        2. Recording their new IDs
        3. Creating experiments with base_exp_id using the new mappings
        4. Migrating events for all experiments

        Args:
            resources: List of experiments to migrate.

        Returns:
            List of migration results.
        """
        if not resources:
            return []

        results = []
        experiments_to_create = []

        # Separate experiments by dependency (base_exp_id)
        independent_experiments = []  # No base_exp_id
        dependent_experiments = []  # Has base_exp_id

        for i, experiment in enumerate(resources):
            if hasattr(experiment, "base_exp_id") and experiment.base_exp_id:
                dependent_experiments.append((i, experiment))
            else:
                independent_experiments.append((i, experiment))

        self._logger.info(
            "Batch experiment creation plan",
            total=len(experiments_to_create),
            independent=len(independent_experiments),
            dependent=len(dependent_experiments),
        )

        # Phase 1: Create independent experiments (no base_exp_id)
        if independent_experiments:
            phase1_results = await self._create_experiments_batch(
                independent_experiments, "independent"
            )
            results.extend(phase1_results)

        # Phase 2: Create dependent experiments (with base_exp_id)
        if dependent_experiments:
            phase2_results = await self._create_experiments_batch(
                dependent_experiments, "dependent"
            )
            results.extend(phase2_results)

        # Phase 3: Migrate events for all successfully created experiments
        successful_migrations = [r for r in results if r.success and not r.skipped]
        if successful_migrations:
            await self._migrate_events_for_experiments(successful_migrations)

        return results

    async def _create_experiments_batch(
        self, indexed_experiments: list[tuple[int, Experiment]], phase_name: str
    ) -> list[MigrationResult]:
        """Create a batch of experiments of the same dependency type.

        Args:
            indexed_experiments: List of (index, experiment) tuples
            phase_name: Name for logging (independent/dependent)

        Returns:
            List of migration results for this batch
        """
        if not indexed_experiments:
            return []

        results = []

        self._logger.info(
            f"Creating {phase_name} experiments",
            count=len(indexed_experiments),
        )

        # Create experiments one by one but track results for bulk event migration
        for index, experiment in indexed_experiments:
            _ = index
            source_id = self.get_resource_id(experiment)

            try:
                # Prepare experiment for creation
                create_params = self.serialize_resource_for_insert(experiment)
                create_params["project_id"] = self.dest_project_id

                # Handle dependencies with ID mapping
                if hasattr(experiment, "base_exp_id") and experiment.base_exp_id:
                    dest_base_exp_id = self.state.id_mapping.get(experiment.base_exp_id)
                    if dest_base_exp_id:
                        create_params["base_exp_id"] = dest_base_exp_id
                    else:
                        self._logger.warning(
                            "Could not resolve base experiment dependency",
                            source_base_exp_id=experiment.base_exp_id,
                        )
                        create_params.pop("base_exp_id", None)

                if hasattr(experiment, "dataset_id") and experiment.dataset_id:
                    dest_dataset_id = self.state.id_mapping.get(experiment.dataset_id)
                    if dest_dataset_id:
                        create_params["dataset_id"] = dest_dataset_id
                    else:
                        self._logger.warning(
                            "Could not resolve dataset dependency",
                            source_dataset_id=experiment.dataset_id,
                        )
                        create_params.pop("dataset_id", None)

                # Create experiment
                dest_experiment = await self.dest_client.with_retry(
                    "create_experiment",
                    lambda create_params=create_params: self.dest_client.client.experiments.create(
                        **create_params
                    ),
                )
                dest_experiment_id = cast(str, cast(Any, dest_experiment).id)

                self._logger.info(
                    f"✅ Created {phase_name} experiment",
                    source_id=source_id,
                    dest_id=dest_experiment_id,
                    name=experiment.name,
                )

                # Record success immediately for dependency resolution
                self.record_success(source_id, dest_experiment_id, experiment)

                results.append(
                    MigrationResult(
                        success=True,
                        source_id=source_id,
                        dest_id=dest_experiment_id,
                        metadata={"name": experiment.name, "events_pending": True},
                    )
                )

            except Exception as e:
                error_msg = f"Failed to create {phase_name} experiment: {e}"
                self._logger.error(
                    error_msg,
                    source_id=source_id,
                    name=experiment.name,
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

    async def _migrate_events_for_experiments(
        self, successful_migrations: list[MigrationResult]
    ) -> None:
        """Migrate events for all successfully created experiments.

        Args:
            successful_migrations: List of successful migration results
        """
        self._logger.info(
            "Starting bulk event migration",
            experiment_count=len(successful_migrations),
        )

        for result in successful_migrations:
            try:
                if result.dest_id is None:
                    raise ValueError(
                        "Experiment migrated without dest_id; cannot copy events"
                    )
                await self._migrate_experiment_events(result.source_id, result.dest_id)

                # Update metadata to indicate events are migrated
                if result.metadata:
                    result.metadata["events_pending"] = False
                    result.metadata["events_migrated"] = True

            except Exception as e:
                self._logger.error(
                    "Failed to migrate events for experiment",
                    source_id=result.source_id,
                    dest_id=result.dest_id,
                    error=str(e),
                )
                # Update metadata to indicate event migration failed
                if result.metadata:
                    result.metadata["events_pending"] = False
                    result.metadata["events_failed"] = True

        self._logger.info(
            "Completed bulk event migration",
            experiment_count=len(successful_migrations),
        )

    async def migrate_resource(self, resource: Experiment) -> str:
        """Migrate a single experiment from source to destination.

        Note: This method is kept for compatibility but migrate_batch should be used
        for better performance when migrating multiple experiments.
        """
        self._logger.info(
            "Migrating experiment",
            source_id=resource.id,
            name=resource.name,
        )

        # Create experiment in destination using base class serialization
        create_params = self.serialize_resource_for_insert(resource)
        create_params["project_id"] = self.dest_project_id

        # Handle dependencies with ID mapping
        if hasattr(resource, "base_exp_id") and resource.base_exp_id:
            dest_base_exp_id = self.state.id_mapping.get(resource.base_exp_id)
            if dest_base_exp_id:
                create_params["base_exp_id"] = dest_base_exp_id
            else:
                self._logger.warning(
                    "Could not resolve base experiment dependency",
                    source_base_exp_id=resource.base_exp_id,
                )
                create_params.pop("base_exp_id", None)

        if hasattr(resource, "dataset_id") and resource.dataset_id:
            dest_dataset_id = self.state.id_mapping.get(resource.dataset_id)
            if dest_dataset_id:
                create_params["dataset_id"] = dest_dataset_id
            else:
                self._logger.warning(
                    "Could not resolve dataset dependency",
                    source_dataset_id=resource.dataset_id,
                )
                create_params.pop("dataset_id", None)

        dest_experiment = await self.dest_client.with_retry(
            "create_experiment",
            lambda create_params=create_params: self.dest_client.client.experiments.create(
                **create_params
            ),
        )
        dest_experiment_id = cast(str, cast(Any, dest_experiment).id)

        # Migrate experiment events
        await self._migrate_experiment_events(resource.id, dest_experiment_id)

        return dest_experiment_id

    async def _migrate_experiment_events(
        self, source_experiment_id: str, dest_experiment_id: str
    ) -> None:
        """Migrate events from source experiment to destination experiment.

        This uses BTQL (SQL) pagination ordered by `_pagination_key` and bounded inserts
        so it can scale to very large experiments (potentially comparable to project logs).

        Deleted events (`_object_delete=true`) are skipped.
        """
        await self._migrate_experiment_events_streaming(
            source_experiment_id, dest_experiment_id
        )

    @dataclass(slots=True)
    class _EventsStreamState:
        version: str | None = None
        cursor: str | None = None
        btql_min_pagination_key: str | None = None
        query_source: str | None = None
        fetched_events: int = 0
        inserted_events: int = 0
        inserted_bytes: int = 0
        skipped_deleted: int = 0
        skipped_seen: int = 0
        attachments_copied: int = 0

        @classmethod
        def from_path(cls, path: Path) -> ExperimentMigrator._EventsStreamState:
            if not path.exists():
                return cls()
            with open(path) as f:
                data = _json.load(f)
            return cls(
                version=data.get("version"),
                cursor=data.get("cursor"),
                btql_min_pagination_key=data.get("btql_min_pagination_key"),
                query_source=data.get("query_source"),
                fetched_events=int(data.get("fetched_events", 0)),
                inserted_events=int(data.get("inserted_events", 0)),
                inserted_bytes=int(data.get("inserted_bytes", 0)),
                skipped_deleted=int(data.get("skipped_deleted", 0)),
                skipped_seen=int(data.get("skipped_seen", 0)),
                attachments_copied=int(data.get("attachments_copied", 0)),
            )

        def to_dict(self) -> dict[str, Any]:
            return {
                "version": self.version,
                "cursor": self.cursor,
                "btql_min_pagination_key": self.btql_min_pagination_key,
                "query_source": self.query_source,
                "fetched_events": self.fetched_events,
                "inserted_events": self.inserted_events,
                "inserted_bytes": self.inserted_bytes,
                "skipped_deleted": self.skipped_deleted,
                "skipped_seen": self.skipped_seen,
                "attachments_copied": self.attachments_copied,
            }

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
        self, event: dict[str, Any], source_experiment_id: str
    ) -> dict[str, Any]:
        out: dict[str, Any] = {
            k: event[k] for k in self._EVENT_INSERT_FIELDS if k in event
        }
        # Ensure id preserved for idempotency
        if "id" in event:
            out["id"] = event["id"]
        # Add provenance if absent
        if "origin" not in out or out.get("origin") is None:
            origin: dict[str, Any] = {
                "object_type": "experiment",
                "object_id": source_experiment_id,
                "id": event.get("id"),
            }
            if event.get("_xact_id") is not None:
                origin["_xact_id"] = event.get("_xact_id")
            if event.get("created") is not None:
                origin["created"] = event.get("created")
            out["origin"] = origin
        return out

    async def _fetch_experiment_events_page(
        self,
        *,
        experiment_id: str,
        cursor: str | None,
        version: str | None,
        limit: int,
        state: _EventsStreamState,
    ) -> dict[str, Any]:
        # Experiments are fetched via BTQL. Cursor/version are intentionally unused.
        _ = cursor
        _ = version
        return await self._fetch_experiment_events_page_btql_sorted(
            experiment_id=experiment_id, limit=limit, state=state
        )

    async def _fetch_experiment_events_page_btql_sorted(
        self,
        *,
        experiment_id: str,
        limit: int,
        state: _EventsStreamState,
    ) -> dict[str, Any]:
        """Fetch one page via POST /btql using SQL syntax, sorted by _pagination_key."""
        last_pagination_key = state.btql_min_pagination_key

        def _query_text_for_limit(n: int) -> str:
            return build_btql_sorted_page_query(
                from_expr=f"experiment('{btql_quote(experiment_id)}', shape => 'spans')",
                limit=n,
                last_pagination_key=last_pagination_key,
                select="*",
            )

        return await fetch_btql_sorted_page_with_retries(
            client=self.source_client,
            query_for_limit=_query_text_for_limit,
            configured_limit=int(limit),
            operation="btql_experiment_events_page",
            log_fields={"source_experiment_id": experiment_id},
            timeout_seconds=120.0,
        )

    async def _insert_experiment_events(
        self, *, experiment_id: str, events: list[dict[str, Any]]
    ) -> None:
        await self.dest_client.with_retry(
            "insert_experiment_events",
            lambda: self.dest_client.raw_request(
                "POST",
                f"/v1/experiment/{experiment_id}/insert",
                json={"events": events},
                timeout=120.0,
            ),
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
        dest_experiment_id: str,
        event: dict[str, Any],
        error: Exception,
    ) -> None:
        event_id = event.get("id")
        safe_id = str(event_id) if isinstance(event_id, str) and event_id else "unknown"
        root_span_id = event.get("root_span_id")
        span_id = event.get("span_id")
        approx_size = self._approx_event_size_bytes(event)
        attachment_refs = self._count_attachment_refs(event)
        path = events_dir / f"oversize_experiment_event_{safe_id}.json"
        summary = {
            "error": str(error),
            "cursor": cursor,
            "dest_experiment_id": dest_experiment_id,
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
                "Oversize experiment event isolated (413). This specific event cannot be inserted.",
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
                "Oversize experiment event isolated; failed to write summary",
                event_id=safe_id,
                root_span_id=root_span_id,
                span_id=span_id,
                cursor=cursor,
            )

    async def _migrate_experiment_events_streaming(
        self, source_experiment_id: str, dest_experiment_id: str
    ) -> None:
        try:
            events_dir = self.checkpoint_dir / "experiment_events"
            events_dir.mkdir(parents=True, exist_ok=True)

            state_path = events_dir / f"{source_experiment_id}_state.json"
            state = self._EventsStreamState.from_path(state_path)

            seen_db = (
                SeenIdsDB(str(events_dir / f"{source_experiment_id}_seen.sqlite3"))
                if self.events_use_seen_db
                else None
            )

            try:
                if state.btql_min_pagination_key is None and state.cursor is not None:
                    # Legacy checkpoint format stored an opaque cursor (from /fetch). We
                    # cannot translate this to a BTQL pagination key. Start from the
                    # beginning and rely on seen_db for idempotency.
                    self._logger.warning(
                        "Experiment events checkpoint contains legacy cursor but no btql_min_pagination_key; restarting BTQL stream from beginning",
                        source_experiment_id=source_experiment_id,
                    )
                    state.cursor = None

                if self.events_use_version_snapshot:
                    self._logger.warning(
                        "Experiment version snapshotting is not supported on some deployments; disabling for this run",
                        source_experiment_id=source_experiment_id,
                    )
                version = None
                progress = self._events_progress_hook

                def _save_state() -> None:
                    state.cursor = None
                    with open(state_path, "w") as f:
                        _json.dump(state.to_dict(), f, indent=2)

                async def _fetch(n: int) -> dict[str, Any]:
                    return await self._fetch_experiment_events_page(
                        experiment_id=source_experiment_id,
                        cursor=None,
                        version=version,
                        limit=n,
                        state=state,
                    )

                async def _on_single_413(event: dict[str, Any], err: Exception) -> None:
                    self._dump_oversize_event_summary(
                        events_dir=events_dir,
                        cursor=state.btql_min_pagination_key,
                        dest_experiment_id=dest_experiment_id,
                        event=event,
                        error=err,
                    )

                await stream_btql_sorted_events(
                    fetch_page=_fetch,
                    page_limit=int(self.events_fetch_limit),
                    get_last_pk=lambda: state.btql_min_pagination_key,
                    set_last_pk=lambda pk: setattr(
                        state, "btql_min_pagination_key", pk
                    ),
                    save_state=_save_state,
                    page_event_filter=lambda e: e.get("_object_delete") is True,
                    event_to_insert=lambda e: self._event_to_insert(
                        e, source_experiment_id
                    ),
                    seen_db=seen_db,
                    insert_batch_size=int(self.events_insert_batch_size),
                    insert_max_bytes=self._insert_max_bytes,
                    rewrite_event_in_place=(
                        None
                        if self._attachment_copier is None
                        else self._attachment_copier.rewrite_event_in_place
                    ),
                    insert_events=lambda batch: self._insert_experiment_events(
                        experiment_id=dest_experiment_id, events=batch
                    ),
                    is_http_413=self._is_http_413,
                    on_single_413=_on_single_413,
                    incr_fetched=lambda n: setattr(
                        state, "fetched_events", int(state.fetched_events) + int(n)
                    ),
                    incr_inserted=lambda n: setattr(
                        state, "inserted_events", int(state.inserted_events) + int(n)
                    ),
                    incr_inserted_bytes=lambda n: setattr(
                        state, "inserted_bytes", int(state.inserted_bytes) + int(n)
                    ),
                    incr_skipped_deleted=lambda n: setattr(
                        state, "skipped_deleted", int(state.skipped_deleted) + int(n)
                    ),
                    incr_skipped_seen=lambda n: setattr(
                        state, "skipped_seen", int(state.skipped_seen) + int(n)
                    ),
                    incr_attachments_copied=lambda n: setattr(
                        state,
                        "attachments_copied",
                        int(state.attachments_copied) + int(n),
                    ),
                    hooks=None
                    if progress is None
                    else {
                        "on_page": lambda info, _p=progress: _p(
                            {
                                "resource": "experiment_events",
                                "phase": "page",
                                "source_experiment_id": source_experiment_id,
                                "dest_experiment_id": dest_experiment_id,
                                "page_num": info.get("page_num"),
                                "page_events": info.get("page_events"),
                                "fetched_total": state.fetched_events,
                                "inserted_total": state.inserted_events,
                                "inserted_bytes_total": state.inserted_bytes,
                                "skipped_deleted_total": state.skipped_deleted,
                                "skipped_seen_total": state.skipped_seen,
                                "attachments_copied_total": state.attachments_copied,
                                "cursor": (
                                    (state.btql_min_pagination_key[:16] + "…")
                                    if isinstance(state.btql_min_pagination_key, str)
                                    else None
                                ),
                                "next_cursor": None,
                            }
                        ),
                        "on_done": lambda _info, _p=progress: _p(
                            {
                                "resource": "experiment_events",
                                "phase": "done",
                                "source_experiment_id": source_experiment_id,
                                "dest_experiment_id": dest_experiment_id,
                                "fetched_total": state.fetched_events,
                                "inserted_total": state.inserted_events,
                                "inserted_bytes_total": state.inserted_bytes,
                                "skipped_deleted_total": state.skipped_deleted,
                                "skipped_seen_total": state.skipped_seen,
                                "attachments_copied_total": state.attachments_copied,
                                "cursor": None,
                                "next_cursor": None,
                            }
                        ),
                    },
                )

                self._logger.info(
                    "Migrated experiment events (streaming)",
                    source_experiment_id=source_experiment_id,
                    dest_experiment_id=dest_experiment_id,
                    fetched=state.fetched_events,
                    inserted=state.inserted_events,
                    skipped_deleted=state.skipped_deleted,
                    skipped_seen=state.skipped_seen,
                )
            finally:
                if seen_db is not None:
                    seen_db.close()

        except Exception as e:
            # Handle specific error cases gracefully
            error_str = str(e)
            if "Error code: 303" in error_str:
                self._logger.warning(
                    "Experiment events fetch returned HTTP 303 - skipping events migration",
                    source_experiment_id=source_experiment_id,
                )
                return
            elif "Error code: 404" in error_str:
                self._logger.info(
                    "No events found in source experiment (404)",
                    source_experiment_id=source_experiment_id,
                )
                return
            else:
                self._logger.error(
                    "Failed to migrate experiment events",
                    source_experiment_id=source_experiment_id,
                    error=str(e),
                )
                raise

    # _prepare_event_for_insertion removed in favor of streaming dict-based copier
