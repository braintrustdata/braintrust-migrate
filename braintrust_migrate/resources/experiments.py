"""Experiment migrator for Braintrust migration tool."""

from __future__ import annotations

import json as _json
import hashlib
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar

from braintrust_migrate.attachments import AttachmentCopier, OversizeFieldSpiller
from braintrust_migrate.btql import (
    btql_quote,
    fetch_btql_sorted_page_with_retries,
)
from braintrust_migrate.resources.base import MigrationResult, ResourceMigrator
from braintrust_migrate.sdk_logs import SDKExperimentWriter
from braintrust_migrate.streaming_utils import (
    STREAMING_EVENT_FETCH_GROUP_SIZE,
    STREAMING_FLUSH_MAX_BYTES,
    STREAMING_FLUSH_MAX_ROWS,
    STREAMING_MAX_EVENT_BYTES,
    EventsStreamState,
    SeenIdsDB,
    StreamingConfig,
    build_btql_sorted_page_query,
    dump_oversize_event_summary,
    is_http_413,
    make_stream_progress_hooks,
    stream_btql_sorted_events_buffered,
)

class ExperimentMigrator(ResourceMigrator[dict]):
    """Migrator for Braintrust experiments.

    Features:
    - Migrates experiment metadata and settings
    - Migrates all experiment events (evaluations) for each experiment
    - Handles experiment dependencies (base_exp_id, dataset_id)
    - Uses bulk operations for better performance

    Uses raw API requests instead of SDK to avoid model dependencies.
    """

    SDK_FLUSH_MAX_ROWS: ClassVar[int] = STREAMING_FLUSH_MAX_ROWS
    SDK_FLUSH_MAX_BYTES: ClassVar[int] = STREAMING_FLUSH_MAX_BYTES
    DEFAULT_EVENT_FETCH_GROUP_SIZE: ClassVar[int] = STREAMING_EVENT_FETCH_GROUP_SIZE
    # Spill individual rows above this size into JSON attachments so they fit
    # under Braintrust's ~20MB per-span logging upload limit (logs3/overflow).
    MAX_EVENT_BYTES: ClassVar[int] = STREAMING_MAX_EVENT_BYTES

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

        stream_cfg = StreamingConfig.resolve(self.source_client, self.dest_client)
        self._sdk_flush_max_rows = stream_cfg.sdk_flush_max_rows
        self._sdk_flush_max_bytes = stream_cfg.sdk_flush_max_bytes
        self._event_fetch_group_size = stream_cfg.event_fetch_group_size
        # Always-on oversize-field spilling (see OversizeFieldSpiller). Only hits
        # the network for rows that exceed the per-span upload limit.
        self._spiller = OversizeFieldSpiller(
            dest_client=self.dest_client,
            max_event_bytes=stream_cfg.max_event_bytes,
        )

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

    # FALLBACK: Static field list for InsertExperimentEvent
    # This is only used if OpenAPI schema is unavailable
    # Primary source: allowed_fields_for_event_insert property (uses OpenAPI spec)
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

    def _sort_experiments_by_dependencies(self, experiments: list[dict]) -> list[dict]:
        """Sort experiments using a simple two-pass approach.

        Pass 1: Experiments without base_exp_id (no dependencies)
        Pass 2: Experiments with base_exp_id (dependent experiments)

        Args:
            experiments: List of experiment dicts to sort

        Returns:
            List of experiments sorted by dependency order
        """
        independent_experiments = []
        dependent_experiments = []

        for exp in experiments:
            if exp.get("base_exp_id"):
                dependent_experiments.append(exp)
            else:
                independent_experiments.append(exp)

        self._logger.info(
            "Split experiments by dependencies",
            independent=len(independent_experiments),
            dependent=len(dependent_experiments),
        )

        return independent_experiments + dependent_experiments

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        """List all experiments from the source organization using raw API, sorted by dependencies."""
        try:
            # Use base class helper method (now uses raw_request)
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

    async def get_dependencies(self, resource: dict) -> list[str]:
        """Get list of resource IDs that this experiment depends on."""
        dependencies = []

        if resource.get("dataset_id"):
            dependencies.append(resource["dataset_id"])

        if resource.get("base_exp_id"):
            dependencies.append(resource["base_exp_id"])

        return dependencies

    async def get_dependency_types(self) -> list[str]:
        """Get list of resource types that experiments might depend on."""
        return ["datasets", "experiments"]

    def _remap_parameters_id(self, resource: dict, create_params: dict) -> None:
        """Remap an experiment's saved-parameters link for the destination.

        ``parameters_id`` is a foreign key to the ``prompts`` table (saved
        parameters are stored as prompts, which migrate before experiments and
        share ``id_mapping``). Forwarding the source id verbatim would hit a
        foreign-key violation, so we remap it to the destination prompt id, or
        drop it (and the paired ``parameters_version``) when the prompt wasn't
        migrated -- mirroring how ``base_exp_id``/``dataset_id`` are handled.
        """
        source_parameters_id = resource.get("parameters_id")
        if not source_parameters_id:
            # No linkage; never send a stray version.
            create_params.pop("parameters_version", None)
            return

        dest_parameters_id = self.state.id_mapping.get(source_parameters_id)
        if dest_parameters_id:
            create_params["parameters_id"] = dest_parameters_id
        else:
            self._logger.debug(
                "Could not resolve saved-parameters dependency; dropping parameters_id",
                source_parameters_id=source_parameters_id,
            )
            create_params.pop("parameters_id", None)
            create_params.pop("parameters_version", None)

    async def migrate_batch(self, resources: list[dict]) -> list[MigrationResult]:
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
            if experiment.get("base_exp_id"):
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
        self, indexed_experiments: list[tuple[int, dict]], phase_name: str
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
                # Prepare creation parameters using OpenAPI allow-list filtering
                create_params = self.serialize_resource_for_insert(experiment)
                create_params["project_id"] = self.dest_project_id

                # Handle dependencies with ID mapping
                if experiment.get("base_exp_id"):
                    dest_base_exp_id = self.state.id_mapping.get(
                        experiment["base_exp_id"]
                    )
                    if dest_base_exp_id:
                        create_params["base_exp_id"] = dest_base_exp_id
                    else:
                        self._logger.debug(
                            "Could not resolve base experiment dependency",
                            source_base_exp_id=experiment["base_exp_id"],
                        )
                        create_params.pop("base_exp_id", None)

                if experiment.get("dataset_id"):
                    dest_dataset_id = self.state.id_mapping.get(
                        experiment["dataset_id"]
                    )
                    if dest_dataset_id:
                        create_params["dataset_id"] = dest_dataset_id
                    else:
                        self._logger.debug(
                            "Could not resolve dataset dependency",
                            source_dataset_id=experiment["dataset_id"],
                        )
                        create_params.pop("dataset_id", None)

                # parameters_id is a FK to the prompts table (saved parameters).
                # Remap it like base_exp_id/dataset_id; drop it (and the paired
                # version) if the referenced prompt wasn't migrated, otherwise the
                # create hits a foreign-key violation in the destination.
                self._remap_parameters_id(experiment, create_params)

                # Create experiment using raw API
                response = await self.dest_client.with_retry(
                    "create_experiment",
                    lambda create_params=create_params: self.dest_client.raw_request(
                        "POST",
                        "/v1/experiment",
                        json=create_params,
                    ),
                )

                dest_experiment_id = response.get("id")
                if not dest_experiment_id:
                    raise ValueError(
                        f"No ID returned when creating experiment {experiment.get('name')}"
                    )

                self._logger.info(
                    f"✅ Created {phase_name} experiment",
                    source_id=source_id,
                    dest_id=dest_experiment_id,
                    name=experiment.get("name"),
                )

                # Record success immediately for dependency resolution
                self.record_success(source_id, dest_experiment_id, experiment)

                results.append(
                    MigrationResult(
                        success=True,
                        source_id=source_id,
                        dest_id=dest_experiment_id,
                        metadata={
                            "name": experiment.get("name"),
                            "events_pending": True,
                        },
                    )
                )

            except Exception as e:
                error_msg = f"Failed to create {phase_name} experiment: {e}"
                self._logger.error(
                    error_msg,
                    source_id=source_id,
                    name=experiment.get("name"),
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

        async def _flush_group(group: list[MigrationResult]) -> None:
            if not group:
                return
            source_to_dest: dict[str, str] = {}
            for result in group:
                if result.dest_id is None:
                    raise ValueError(
                        "Experiment migrated without dest_id; cannot copy events"
                    )
                source_to_dest[result.source_id] = result.dest_id

            await self._migrate_experiment_events_streaming_grouped(source_to_dest)

            for result in group:
                if result.metadata:
                    result.metadata["events_pending"] = False
                    result.metadata["events_migrated"] = True

        for start in range(0, len(successful_migrations), self._event_fetch_group_size):
            group = successful_migrations[start : start + self._event_fetch_group_size]
            try:
                await _flush_group(group)
            except Exception as e:
                self._logger.error(
                    "Failed to migrate events for experiment group",
                    source_ids=[result.source_id for result in group],
                    error=str(e),
                )
                for result in group:
                    if result.metadata:
                        result.metadata["events_pending"] = False
                        result.metadata["events_failed"] = True

        self._logger.info(
            "Completed bulk event migration",
            experiment_count=len(successful_migrations),
        )

    async def migrate_resource(self, resource: dict) -> str:
        """Migrate a single experiment from source to destination using raw API.

        Note: This method is kept for compatibility but migrate_batch should be used
        for better performance when migrating multiple experiments.
        """
        self._logger.info(
            "Migrating experiment",
            source_id=resource.get("id"),
            name=resource.get("name"),
        )

        # Prepare creation parameters using OpenAPI allow-list filtering
        create_params = self.serialize_resource_for_insert(resource)
        create_params["project_id"] = self.dest_project_id

        # Handle dependencies with ID mapping
        if resource.get("base_exp_id"):
            dest_base_exp_id = self.state.id_mapping.get(resource["base_exp_id"])
            if dest_base_exp_id:
                create_params["base_exp_id"] = dest_base_exp_id
            else:
                self._logger.debug(
                    "Could not resolve base experiment dependency",
                    source_base_exp_id=resource["base_exp_id"],
                )
                create_params.pop("base_exp_id", None)

        if resource.get("dataset_id"):
            dest_dataset_id = self.state.id_mapping.get(resource["dataset_id"])
            if dest_dataset_id:
                create_params["dataset_id"] = dest_dataset_id
            else:
                self._logger.debug(
                    "Could not resolve dataset dependency",
                    source_dataset_id=resource["dataset_id"],
                )
                create_params.pop("dataset_id", None)

        # parameters_id is a FK to the prompts table (saved parameters); remap or
        # drop it like the other dependencies (see _remap_parameters_id).
        self._remap_parameters_id(resource, create_params)

        # Create experiment using raw API
        response = await self.dest_client.with_retry(
            "create_experiment",
            lambda create_params=create_params: self.dest_client.raw_request(
                "POST",
                "/v1/experiment",
                json=create_params,
            ),
        )

        dest_experiment_id = response.get("id")
        if not dest_experiment_id:
            raise ValueError(
                f"No ID returned when creating experiment {resource.get('name')}"
            )

        # Migrate experiment events
        await self._migrate_experiment_events(resource["id"], dest_experiment_id)

        return dest_experiment_id

    async def _migrate_experiment_events(
        self, source_experiment_id: str, dest_experiment_id: str
    ) -> None:
        """Migrate events from source experiment to destination experiment.

        This uses native BTQL pagination ordered by `_pagination_key` and bounded inserts
        so it can scale to very large experiments (potentially comparable to project logs).

        Deleted events (`_object_delete=true`) are skipped.
        """
        await self._migrate_experiment_events_streaming_grouped(
            {source_experiment_id: dest_experiment_id}
        )

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
        # Use dynamic schema from OpenAPI instead of static list
        allowed_fields = self.allowed_fields_for_event_insert
        if allowed_fields:
            out: dict[str, Any] = {k: event[k] for k in allowed_fields if k in event}
        else:
            # Fallback to static list if OpenAPI schema not available
            out = {k: event[k] for k in self._EVENT_INSERT_FIELDS if k in event}

        # Ensure id preserved for idempotency
        if "id" in event:
            out["id"] = event["id"]
        out["experiment_id"] = source_experiment_id
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

    def _event_to_insert_from_row(self, event: dict[str, Any]) -> dict[str, Any]:
        source_experiment_id = event.get("experiment_id")
        if not isinstance(source_experiment_id, str) or not source_experiment_id:
            raise ValueError("Fetched experiment event missing experiment_id")
        return self._event_to_insert(event, source_experiment_id)

    async def _fetch_experiment_events_page(
        self,
        *,
        experiment_id: str,
        cursor: str | None,
        version: str | None,
        limit: int,
        state: EventsStreamState,
    ) -> dict[str, Any]:
        # Experiments are fetched via BTQL. Cursor/version are intentionally unused.
        _ = cursor
        _ = version
        return await self._fetch_experiment_events_page_btql_sorted(
            experiment_ids=[experiment_id], limit=limit, state=state
        )

    async def _fetch_experiment_events_page_btql_sorted(
        self,
        *,
        experiment_ids: list[str],
        limit: int,
        state: EventsStreamState,
    ) -> dict[str, Any]:
        """Fetch one page via POST /btql using native BTQL syntax, sorted by _pagination_key."""
        last_pagination_key = state.btql_min_pagination_key
        quoted_ids = ", ".join(f"'{btql_quote(experiment_id)}'" for experiment_id in experiment_ids)

        def _query_text_for_limit(n: int) -> str:
            return build_btql_sorted_page_query(
                from_expr=f"experiment({quoted_ids}) spans",
                limit=n,
                last_pagination_key=last_pagination_key,
                select="*",
            )

        return await fetch_btql_sorted_page_with_retries(
            client=self.source_client,
            query_for_limit=_query_text_for_limit,
            configured_limit=int(limit),
            operation="btql_experiment_events_page",
            log_fields={"source_experiment_ids": experiment_ids},
            timeout_seconds=120.0,
        )

    @staticmethod
    def _group_stream_basename(source_experiment_ids: list[str]) -> str:
        if len(source_experiment_ids) == 1:
            return source_experiment_ids[0]
        joined = ",".join(source_experiment_ids)
        digest = hashlib.sha1(joined.encode("utf-8")).hexdigest()[:12]
        return f"group_{digest}"

    async def _insert_experiment_events_grouped(
        self,
        *,
        batch: list[dict[str, Any]],
        writers_by_source: dict[str, SDKExperimentWriter],
    ) -> None:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for event in batch:
            source_experiment_id = event.get("experiment_id")
            if not isinstance(source_experiment_id, str) or not source_experiment_id:
                raise ValueError("Experiment event batch missing source experiment_id")
            grouped.setdefault(source_experiment_id, []).append(event)

        for source_experiment_id, rows in grouped.items():
            writer = writers_by_source.get(source_experiment_id)
            if writer is None:
                raise KeyError(
                    f"No destination writer for source experiment {source_experiment_id}"
                )
            await self.dest_client.with_retry(
                "insert_experiment_events",
                lambda rows=rows, writer=writer: writer.write_rows(rows),
            )

    async def _migrate_experiment_events_streaming_grouped(
        self, source_to_dest_experiment_ids: dict[str, str]
    ) -> None:
        try:
            events_dir = self.checkpoint_dir / "experiment_events"
            events_dir.mkdir(parents=True, exist_ok=True)
            source_experiment_ids = list(source_to_dest_experiment_ids.keys())
            group_basename = self._group_stream_basename(source_experiment_ids)

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
                        "Experiment events checkpoint contains legacy cursor but no btql_min_pagination_key; restarting BTQL stream from beginning",
                        source_experiment_ids=source_experiment_ids,
                    )
                    state.cursor = None

                progress = self._events_progress_hook
                writers_by_source = {
                    source_experiment_id: SDKExperimentWriter(
                        self.dest_client, dest_experiment_id
                    )
                    for source_experiment_id, dest_experiment_id in source_to_dest_experiment_ids.items()
                }

                def _save_state() -> None:
                    state.cursor = None
                    with open(state_path, "w") as f:
                        _json.dump(state.to_dict(), f, indent=2)

                async def _fetch(n: int) -> dict[str, Any]:
                    return await self._fetch_experiment_events_page_btql_sorted(
                        experiment_ids=source_experiment_ids,
                        limit=n,
                        state=state,
                    )

                async def _on_single_413(event: dict[str, Any], err: Exception) -> None:
                    source_experiment_id = event.get("experiment_id")
                    dump_oversize_event_summary(
                        out_dir=events_dir,
                        filename_prefix="oversize_experiment_event_",
                        event_label="experiment event",
                        dest_id_field="dest_experiment_id",
                        dest_id_value=(
                            source_to_dest_experiment_ids.get(source_experiment_id)
                            if isinstance(source_experiment_id, str)
                            else None
                        )
                        or "unknown",
                        cursor=state.btql_min_pagination_key,
                        event=event,
                        error=err,
                        logger=self._logger,
                    )

                await stream_btql_sorted_events_buffered(
                    fetch_page=_fetch,
                    page_limit=int(self.events_fetch_limit),
                    state=state,
                    save_state=_save_state,
                    page_event_filter=lambda e: e.get("_object_delete") is True,
                    event_to_insert=self._event_to_insert_from_row,
                    seen_db=seen_db,
                    rewrite_event_in_place=(
                        None
                        if self._attachment_copier is None
                        else self._attachment_copier.rewrite_event_in_place
                    ),
                    spill_event_in_place=self._spiller.spill_event_in_place,
                    insert_events=lambda batch: self._insert_experiment_events_grouped(
                        batch=batch,
                        writers_by_source=writers_by_source,
                    ),
                    flush_max_rows=self._sdk_flush_max_rows,
                    flush_max_bytes=self._sdk_flush_max_bytes,
                    is_http_413=is_http_413,
                    on_single_413=_on_single_413,
                    hooks=make_stream_progress_hooks(
                        progress,
                        state,
                        resource="experiment_events",
                        id_fields={
                            "source_experiment_ids": source_experiment_ids,
                            "dest_experiment_ids": list(
                                source_to_dest_experiment_ids.values()
                            ),
                        },
                    ),
                )

                self._logger.info(
                    "Migrated experiment events (streaming)",
                    source_experiment_ids=source_experiment_ids,
                    dest_experiment_ids=list(source_to_dest_experiment_ids.values()),
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
                    source_experiment_ids=list(source_to_dest_experiment_ids.keys()),
                )
                return
            elif "Error code: 404" in error_str:
                self._logger.info(
                    "No events found in source experiment (404)",
                    source_experiment_ids=list(source_to_dest_experiment_ids.keys()),
                )
                return
            else:
                self._logger.error(
                    "Failed to migrate experiment events",
                    source_experiment_ids=list(source_to_dest_experiment_ids.keys()),
                    error=str(e),
                )
                raise

    # _prepare_event_for_insertion removed in favor of streaming dict-based copier
