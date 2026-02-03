"""Migration orchestrator for coordinating Braintrust resource migrations."""

import asyncio
import json
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar, TypeVar, cast

import structlog
from braintrust_api.types import Project

from braintrust_migrate.client import BraintrustClient, create_client_pair
from braintrust_migrate.config import Config
from braintrust_migrate.resources import (
    AISecretMigrator,
    DatasetMigrator,
    ExperimentMigrator,
    FunctionMigrator,
    GroupMigrator,
    LogsMigrator,
    ProjectScoreMigrator,
    ProjectTagMigrator,
    PromptMigrator,
    RoleMigrator,
    SpanIframeMigrator,
    ViewMigrator,
)

logger = structlog.get_logger(__name__)

# Human-readable summary truncation limits
MAX_SUMMARY_ITEMS_PER_REASON: int = 10

T = TypeVar("T")


async def _gather_with_concurrency(
    coros: Sequence[Awaitable[T]], *, max_concurrent: int
) -> list[T | Exception]:
    """Run awaitables with bounded concurrency.

    Returns a list aligned with the input order. Any exception is captured and
    returned as an Exception element (i.e. this will not raise).
    """
    sem = asyncio.Semaphore(max_concurrent)

    async def _run(coro: Awaitable[T]) -> T:
        async with sem:
            return await coro

    return cast(
        list[T | Exception],
        await asyncio.gather(*(_run(c) for c in coros), return_exceptions=True),
    )


class MigrationOrchestrator:
    """Orchestrates the migration of Braintrust resources in the correct order.

    Handles:
    - Project discovery and creation
    - Resource migration in dependency order
    - Progress tracking and reporting
    - Error handling and recovery
    """

    # Define migration order based on dependencies and scope
    # Organization-scoped resources are migrated once for the entire migration
    # Project-scoped resources are migrated for each project

    # Organization-scoped resources (migrated once, globally)
    ORGANIZATION_SCOPED_RESOURCES: ClassVar[list[tuple[str, type]]] = [
        ("ai_secrets", AISecretMigrator),  # AI provider credentials - no dependencies
        ("roles", RoleMigrator),  # Organization roles - may be referenced by ACLs
        ("groups", GroupMigrator),  # Organization groups - may be referenced by ACLs
    ]

    # Project-scoped resources (migrated per project)
    PROJECT_SCOPED_RESOURCES: ClassVar[list[tuple[str, type]]] = [
        ("datasets", DatasetMigrator),
        ("project_tags", ProjectTagMigrator),  # Only depend on projects, migrated early
        ("span_iframes", SpanIframeMigrator),  # No dependencies, project-scoped
        (
            "functions",
            FunctionMigrator,
        ),  # Functions (tools, scorers, tasks, LLMs) - all encompassing
        (
            "prompts",
            PromptMigrator,
        ),  # Prompts can depend on functions from previous step
        (
            "project_scores",
            ProjectScoreMigrator,
        ),  # Project scores can depend on functions for online scoring
        ("experiments", ExperimentMigrator),
        ("logs", LogsMigrator),
        ("views", ViewMigrator),
        # ("acls", ACLMigrator),  # Last - depends on all other resources
    ]

    # Combined migration order for backward compatibility and resource filtering
    MIGRATION_ORDER: ClassVar[list[tuple[str, type]]] = (
        ORGANIZATION_SCOPED_RESOURCES + PROJECT_SCOPED_RESOURCES
    )

    def __init__(self, config: Config) -> None:
        """Initialize the migration orchestrator.

        Args:
            config: Migration configuration.
        """
        self.config = config
        self._logger = logger.bind(orchestrator=True)

    async def migrate_all(self) -> dict[str, Any]:
        """Migrate all resources from source to destination organization.

        Returns:
            Summary of migration results.
        """
        start_time = datetime.now()
        self._logger.info("Starting complete migration")

        # Create timestamped checkpoint directory
        timestamp = start_time.strftime("%Y%m%d_%H%M%S")
        checkpoint_dir = self.config.ensure_checkpoint_dir() / timestamp

        total_results = {
            "start_time": start_time.isoformat(),
            "checkpoint_dir": str(checkpoint_dir),
            "organization_resources": {},  # NEW: Track org-scoped resources separately
            "projects": {},
            "summary": {
                "total_projects": 0,
                "total_resources": 0,
                "migrated_resources": 0,
                "skipped_resources": 0,
                "failed_resources": 0,
                "errors": [],
            },
        }

        try:
            async with create_client_pair(
                self.config.source,
                self.config.destination,
                self.config.migration,
            ) as (source_client, dest_client):
                # Discover projects
                projects = await self._discover_projects(source_client, dest_client)
                total_results["summary"]["total_projects"] = len(projects)

                self._logger.info(f"Discovered {len(projects)} projects to migrate")

                # Create global ID mapping registry to share between ALL projects
                global_id_mappings = {}

                # Pre-populate with project mappings
                for project in projects:
                    global_id_mappings[project["source_id"]] = project["dest_id"]
                    self._logger.debug(
                        "Added project mapping",
                        source_project_id=project["source_id"],
                        dest_project_id=project["dest_id"],
                        project_name=project["name"],
                    )

                # STEP 1: Migrate organization-scoped resources once
                self._logger.info("Migrating organization-scoped resources")
                org_results = await self._migrate_organization_resources(
                    source_client,
                    dest_client,
                    checkpoint_dir,
                    global_id_mappings,
                )

                total_results["organization_resources"] = org_results

                # Aggregate organization resource results
                summary = total_results["summary"]
                summary["total_resources"] += org_results.get("total_resources", 0)
                summary["migrated_resources"] += org_results.get(
                    "migrated_resources", 0
                )
                summary["skipped_resources"] += org_results.get("skipped_resources", 0)
                summary["failed_resources"] += org_results.get("failed_resources", 0)
                summary["errors"].extend(org_results.get("errors", []))

                # STEP 2: Migrate project-scoped resources for each project
                self._logger.info("Migrating project-scoped resources")
                max_concurrent = int(self.config.migration.max_concurrent)
                self._logger.info(
                    "Starting bounded project migrations",
                    max_concurrent=max_concurrent,
                    total_projects=len(projects),
                )

                project_coros = [
                    self._migrate_project(
                        project,
                        source_client,
                        dest_client,
                        checkpoint_dir,
                        global_id_mappings,  # shared mappings across all projects
                    )
                    for project in projects
                ]
                project_results_list = await _gather_with_concurrency(
                    project_coros, max_concurrent=max_concurrent
                )

                for project, res in zip(projects, project_results_list, strict=True):
                    if isinstance(res, Exception):
                        error_msg = f"Project migration failed: {res}"
                        self._logger.error(
                            error_msg,
                            project=project.get("name"),
                            source_project_id=project.get("source_id"),
                            dest_project_id=project.get("dest_id"),
                        )
                        project_results = {
                            "project_id": project.get("dest_id"),
                            "project_name": project.get("name"),
                            "resources": {},
                            "total_resources": 0,
                            "migrated_resources": 0,
                            "skipped_resources": 0,
                            "failed_resources": 0,
                            "errors": [
                                {"resource_type": "project", "error": error_msg}
                            ],
                        }
                    else:
                        project_results = cast(dict[str, Any], res)

                    total_results["projects"][project["name"]] = project_results

                    # Aggregate project results
                    summary["total_resources"] += project_results.get(
                        "total_resources", 0
                    )
                    summary["migrated_resources"] += project_results.get(
                        "migrated_resources", 0
                    )
                    summary["skipped_resources"] += project_results.get(
                        "skipped_resources", 0
                    )
                    summary["failed_resources"] += project_results.get(
                        "failed_resources", 0
                    )
                    summary["errors"].extend(project_results.get("errors", []))

        except Exception as e:
            self._logger.error("Migration failed", error=str(e))
            total_results["summary"]["errors"].append(
                {
                    "type": "orchestrator_error",
                    "error": str(e),
                }
            )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        total_results.update(
            {
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "success": total_results["summary"]["failed_resources"] == 0
                and len(total_results["summary"]["errors"]) == 0,
            }
        )

        # Generate detailed migration report
        report_path = self._generate_migration_report(total_results, checkpoint_dir)
        total_results["report_path"] = str(report_path)

        self._logger.info(
            "Migration completed",
            duration_seconds=duration,
            total_resources=total_results["summary"]["total_resources"],
            migrated=total_results["summary"]["migrated_resources"],
            skipped=total_results["summary"]["skipped_resources"],
            failed=total_results["summary"]["failed_resources"],
            report_path=str(report_path),
        )

        return total_results

    async def _discover_projects(
        self,
        source_client: BraintrustClient,
        dest_client: BraintrustClient,
    ) -> list[dict[str, Any]]:
        """Discover projects in source organization and ensure they exist in destination.

        Args:
            source_client: Source organization client.
            dest_client: Destination organization client.

        Returns:
            List of project information dictionaries.
        """
        self._logger.info("Discovering projects")

        # List projects from source
        source_projects = await source_client.with_retry(
            "list_source_projects", lambda: source_client.client.projects.list()
        )

        projects = []

        if source_projects is None:
            projects = []
        # Convert to list if it's an async iterator
        elif hasattr(source_projects, "__aiter__"):
            async for project in source_projects:
                projects.append(project)
        else:
            projects = list(source_projects)

        # Filter projects if project_names is specified
        if self.config.project_names:
            filtered_projects = []
            project_names_set = set(self.config.project_names)

            for project in projects:
                if project.name in project_names_set:
                    filtered_projects.append(project)

            # Log which projects were found and which were not
            found_names = {project.name for project in filtered_projects}
            missing_names = project_names_set - found_names

            if missing_names:
                self._logger.warning(
                    "Some specified projects were not found in source organization",
                    missing_projects=list(missing_names),
                    available_projects=[p.name for p in projects],
                )

            self._logger.info(
                "Filtered projects based on configuration",
                requested_projects=self.config.project_names,
                found_projects=list(found_names),
                total_available=len(projects),
                total_selected=len(filtered_projects),
            )

            projects = filtered_projects

        # Ensure projects exist in destination and get destination IDs
        project_mappings = []
        for project in projects:
            dest_project_id = await self._ensure_project_exists(project, dest_client)
            project_mappings.append(
                {
                    "source_id": project.id,
                    "dest_id": dest_project_id,
                    "name": project.name,
                    "description": getattr(project, "description", None),
                }
            )

        return project_mappings

    async def _ensure_project_exists(
        self,
        source_project: Project,
        dest_client: BraintrustClient,
    ) -> str:
        """Ensure a project exists in the destination organization.

        Args:
            source_project: Source project to replicate.
            dest_client: Destination client.

        Returns:
            Destination project ID.
        """
        try:
            # Check if project already exists
            dest_projects = await dest_client.with_retry(
                "list_dest_projects", lambda: dest_client.client.projects.list()
            )

            # Convert to list and check if project exists
            existing_project = None
            if dest_projects is None:
                existing_project = None
            elif hasattr(dest_projects, "__aiter__"):
                async for dest_project in dest_projects:
                    if dest_project.name == source_project.name:
                        existing_project = dest_project
                        break
            else:
                for dest_project in dest_projects:
                    if dest_project.name == source_project.name:
                        existing_project = dest_project
                        break

            if existing_project:
                self._logger.debug(
                    "Project already exists in destination",
                    project_name=source_project.name,
                    dest_id=existing_project.id,
                )
                return existing_project.id

            # Create project in destination
            # Note: description parameter is not supported by projects.create() in braintrust-api
            create_params = {"name": source_project.name}

            new_project = cast(
                Any,
                await dest_client.with_retry(
                    "create_project",
                    # braintrust-api client is dynamically generated; use Any to avoid type noise
                    lambda: cast(Any, dest_client.client.projects).create(
                        **create_params
                    ),
                ),
            )

            new_project_id = cast(str, new_project.id)

            self._logger.info(
                "Created project in destination",
                project_name=source_project.name,
                source_id=source_project.id,
                dest_id=new_project_id,
            )

            return new_project_id

        except Exception as e:
            self._logger.error(
                "Failed to ensure project exists",
                project_name=source_project.name,
                error=str(e),
            )
            raise

    async def _migrate_project(
        self,
        project: dict[str, Any],
        source_client: BraintrustClient,
        dest_client: BraintrustClient,
        checkpoint_dir: Path,
        global_id_mappings: dict[str, str],
        progress_factory: Callable[[str], Callable[[dict[str, Any]], None]]
        | None = None,
        resource_callback: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate all resources for a specific project.

        Args:
            project: Project information.
            source_client: Source organization client.
            dest_client: Destination organization client.
            checkpoint_dir: Directory for storing checkpoints.
            global_id_mappings: Global ID mappings shared across all projects.
            progress_factory: Factory for creating streaming progress hooks.
            resource_callback: Called after each resource type completes with
                (resource_name, results_dict) for real-time feedback.

        Returns:
            Migration results for the project.
        """
        project_name = project["name"]
        source_project_id = project["source_id"]
        dest_project_id = project["dest_id"]

        self._logger.info(
            f"Starting migration for project: {project_name}",
            source_project_id=source_project_id,
            dest_project_id=dest_project_id,
        )

        # Create project-specific checkpoint directory
        project_checkpoint_dir = checkpoint_dir / project_name
        project_checkpoint_dir.mkdir(parents=True, exist_ok=True)

        project_results = {
            "project_id": dest_project_id,  # Use destination project ID in results
            "project_name": project_name,
            "resources": {},
            "total_resources": 0,
            "migrated_resources": 0,
            "skipped_resources": 0,
            "failed_resources": 0,
            "errors": [],
        }

        # Filter project-scoped resources to migrate based on config
        resources_to_migrate = self._get_project_resources_to_migrate()

        # Create shared dependency cache for efficiency
        shared_dependency_cache = {}

        # Migrate project-scoped resources in dependency order
        for resource_name, migrator_class in self.PROJECT_SCOPED_RESOURCES:
            # Filter project-scoped resources to migrate based on config
            if resource_name not in resources_to_migrate:
                self._logger.debug(f"Skipping {resource_name} (not in migration list)")
                continue

            migrator = None
            try:
                self._logger.info(
                    f"Migrating {resource_name} for project {project_name}"
                )

                # Create migrator instance
                # Only create progress hooks for streaming resources that will use them
                is_streaming_resource = resource_name in (
                    "logs",
                    "datasets",
                    "experiments",
                )
                progress_hook = (
                    progress_factory(resource_name)
                    if progress_factory is not None and is_streaming_resource
                    else None
                )
                if resource_name == "logs":
                    migrator = LogsMigrator(
                        source_client,
                        dest_client,
                        project_checkpoint_dir,
                        page_limit=self.config.migration.logs_fetch_limit,
                        insert_batch_size=self.config.migration.logs_insert_batch_size,
                        use_version_snapshot=self.config.migration.logs_use_version_snapshot,
                        use_seen_db=self.config.migration.logs_use_seen_db,
                        progress_hook=progress_hook,
                    )
                elif resource_name == "datasets":
                    migrator = migrator_class(
                        source_client,
                        dest_client,
                        project_checkpoint_dir,
                        self.config.migration.batch_size,
                        events_fetch_limit=self.config.migration.dataset_events_fetch_limit,
                        events_insert_batch_size=self.config.migration.dataset_events_insert_batch_size,
                        events_use_version_snapshot=self.config.migration.dataset_events_use_version_snapshot,
                        events_use_seen_db=self.config.migration.dataset_events_use_seen_db,
                        events_progress_hook=progress_hook,
                    )
                elif resource_name == "experiments":
                    # Experiments can be logs-scale; stream their events with dedicated knobs.
                    migrator = migrator_class(
                        source_client,
                        dest_client,
                        project_checkpoint_dir,
                        self.config.migration.batch_size,
                        events_fetch_limit=self.config.migration.experiment_events_fetch_limit,
                        events_insert_batch_size=self.config.migration.experiment_events_insert_batch_size,
                        events_use_version_snapshot=self.config.migration.experiment_events_use_version_snapshot,
                        events_use_seen_db=self.config.migration.experiment_events_use_seen_db,
                        events_progress_hook=progress_hook,
                    )
                else:
                    migrator = migrator_class(
                        source_client,
                        dest_client,
                        project_checkpoint_dir,
                        self.config.migration.batch_size,
                    )

                # Set the destination project ID for the migrator
                migrator.set_destination_project_id(dest_project_id)

                # Share ID mappings from previous migrators
                migrator.update_id_mappings(global_id_mappings)

                # Pre-populate dependency mappings before migration
                await migrator.populate_dependency_mappings(
                    source_client,
                    dest_client,
                    source_project_id,
                    shared_dependency_cache,
                )

                # Perform migration
                resource_results = await migrator.migrate_all(source_project_id)

                # Collect ID mappings from this migrator and add to global registry
                new_mappings = {
                    k: v
                    for k, v in migrator.state.id_mapping.items()
                    if k not in global_id_mappings  # Only add new mappings
                }
                global_id_mappings.update(migrator.state.id_mapping)

                if new_mappings:
                    self._logger.debug(
                        f"Added {len(new_mappings)} new ID mappings from {migrator_class.__name__}",
                        new_mappings_count=len(new_mappings),
                    )

                # Store results
                project_results["resources"][resource_name] = resource_results

                # Aggregate results
                project_results["total_resources"] += resource_results["total"]
                project_results["migrated_resources"] += resource_results["migrated"]
                project_results["skipped_resources"] += resource_results["skipped"]
                project_results["failed_resources"] += resource_results["failed"]
                project_results["errors"].extend(resource_results["errors"])

                self._logger.info(
                    f"Completed {resource_name} migration for project {project_name}",
                    total=resource_results["total"],
                    migrated=resource_results["migrated"],
                    skipped=resource_results["skipped"],
                    failed=resource_results["failed"],
                )

                # Notify callback for real-time console feedback
                if resource_callback is not None:
                    resource_callback(resource_name, resource_results)

            except Exception as e:
                err_str = str(e)
                detail = err_str if err_str else repr(e)
                error_msg = (
                    f"Failed to migrate {resource_name}: {type(e).__name__}: {detail}"
                )
                self._logger.error(
                    error_msg,
                    project=project_name,
                    resource=resource_name,
                    error_type=type(e).__name__,
                    error_repr=repr(e),
                )

                # Best-effort: capture partial progress for streaming migrators so the
                # migration_results.json shows what was actually moved before failure.
                partial: dict[str, Any] | None = None
                try:
                    if migrator is not None and hasattr(
                        migrator, "get_partial_results"
                    ):
                        partial = migrator.get_partial_results()  # type: ignore[no-any-return]
                except Exception:
                    partial = None

                if partial is not None:
                    # Attach error to partial result and store it.
                    partial_errors = list(partial.get("errors") or [])
                    partial_errors.append(
                        {
                            "error": error_msg,
                            "error_type": type(e).__name__,
                            "error_repr": repr(e),
                        }
                    )
                    partial["errors"] = partial_errors
                    project_results["resources"][resource_name] = partial
                    # Aggregate partials so summary reflects real progress.
                    project_results["total_resources"] += int(partial.get("total", 0))
                    project_results["migrated_resources"] += int(
                        partial.get("migrated", 0)
                    )
                    project_results["skipped_resources"] += int(
                        partial.get("skipped", 0)
                    )
                    project_results["failed_resources"] += int(partial.get("failed", 0))
                    project_results["errors"].extend(partial_errors)
                else:
                    project_results["errors"].append(
                        {
                            "resource_type": resource_name,
                            "error": error_msg,
                            "error_type": type(e).__name__,
                            "error_repr": repr(e),
                        }
                    )

        self._logger.info(
            f"Completed project migration: {project_name}",
            total_resources=project_results["total_resources"],
            migrated=project_results["migrated_resources"],
            skipped=project_results["skipped_resources"],
            failed=project_results["failed_resources"],
        )

        return project_results

    def _get_organization_resources_to_migrate(self) -> list[str]:
        """Get list of organization-scoped resource types to migrate based on configuration.

        Returns:
            List of organization-scoped resource type names to migrate.
        """
        if "all" in self.config.resources:
            return [name for name, _ in self.ORGANIZATION_SCOPED_RESOURCES]
        else:
            # Filter based on specified resources
            available_org_resources = {
                name for name, _ in self.ORGANIZATION_SCOPED_RESOURCES
            }
            return [r for r in self.config.resources if r in available_org_resources]

    def _get_project_resources_to_migrate(self) -> list[str]:
        """Get list of project-scoped resource types to migrate based on configuration.

        Returns:
            List of project-scoped resource type names to migrate.
        """
        if "all" in self.config.resources:
            return [name for name, _ in self.PROJECT_SCOPED_RESOURCES]
        else:
            # Filter based on specified resources
            available_project_resources = {
                name for name, _ in self.PROJECT_SCOPED_RESOURCES
            }
            return [
                r for r in self.config.resources if r in available_project_resources
            ]

    def _get_resources_to_migrate(self) -> list[str]:
        """Get list of resource types to migrate based on configuration.

        Returns:
            List of resource type names to migrate (backward compatibility).
        """
        if "all" in self.config.resources:
            return [name for name, _ in self.MIGRATION_ORDER]
        else:
            # Filter based on specified resources
            available_resources = {name for name, _ in self.MIGRATION_ORDER}
            return [r for r in self.config.resources if r in available_resources]

    def _generate_migration_report(
        self, results: dict[str, Any], checkpoint_dir: Path
    ) -> Path:
        """Generate a detailed migration report.

        Args:
            results: Migration results from migrate_all().
            checkpoint_dir: Directory to save the report.

        Returns:
            Path to the generated report file.
        """
        report_path = checkpoint_dir / "migration_report.json"

        # Create detailed report structure
        detailed_report = {
            "migration_summary": {
                "start_time": results.get("start_time"),
                "end_time": results.get("end_time"),
                "duration_seconds": results.get("duration_seconds"),
                "success": results.get("success"),
                "total_projects": results["summary"]["total_projects"],
                "total_resources": results["summary"]["total_resources"],
                "migrated_resources": results["summary"]["migrated_resources"],
                "skipped_resources": results["summary"]["skipped_resources"],
                "failed_resources": results["summary"]["failed_resources"],
            },
            "organization_resources": {},
            "projects": {},
            "detailed_breakdown": {
                "migrated": [],
                "skipped": [],
                "failed": [],
            },
        }

        # Process organization resources
        org_data = results.get("organization_resources", {})
        if org_data:
            org_summary = {
                "total_resources": org_data.get("total_resources", 0),
                "migrated_resources": org_data.get("migrated_resources", 0),
                "skipped_resources": org_data.get("skipped_resources", 0),
                "failed_resources": org_data.get("failed_resources", 0),
                "resources": {},
            }

            # Process each organization resource type
            for resource_type, resource_data in org_data.get("resources", {}).items():
                org_summary["resources"][resource_type] = {
                    "total": resource_data.get("total", 0),
                    "migrated": resource_data.get("migrated", 0),
                    "skipped": resource_data.get("skipped", 0),
                    "failed": resource_data.get("failed", 0),
                }

                # Add to detailed breakdown
                for detail in resource_data.get("migrated_details", []):
                    detailed_report["detailed_breakdown"]["migrated"].append(
                        {
                            "scope": "organization",
                            "resource_type": resource_type,
                            "source_id": detail["source_id"],
                            "dest_id": detail["dest_id"],
                            "name": detail["name"],
                        }
                    )

                for detail in resource_data.get("skipped_details", []):
                    detailed_report["detailed_breakdown"]["skipped"].append(
                        {
                            "scope": "organization",
                            "resource_type": resource_type,
                            "source_id": detail["source_id"],
                            "dest_id": detail.get("dest_id"),
                            "name": detail["name"],
                            "skip_reason": detail["skip_reason"],
                        }
                    )

                for error in resource_data.get("errors", []):
                    detailed_report["detailed_breakdown"]["failed"].append(
                        {
                            "scope": "organization",
                            "resource_type": resource_type,
                            "source_id": error["source_id"],
                            "name": error.get("name"),
                            "error": error["error"],
                        }
                    )

            detailed_report["organization_resources"] = org_summary

        # Process each project
        for project_name, project_data in results.get("projects", {}).items():
            project_summary = {
                "project_name": project_name,
                "project_id": project_data.get("project_id"),
                "total_resources": project_data.get("total_resources", 0),
                "migrated_resources": project_data.get("migrated_resources", 0),
                "skipped_resources": project_data.get("skipped_resources", 0),
                "failed_resources": project_data.get("failed_resources", 0),
                "resources": {},
            }

            # Process each resource type in the project
            for resource_type, resource_data in project_data.get(
                "resources", {}
            ).items():
                project_summary["resources"][resource_type] = {
                    "total": resource_data.get("total", 0),
                    "migrated": resource_data.get("migrated", 0),
                    "skipped": resource_data.get("skipped", 0),
                    "failed": resource_data.get("failed", 0),
                }

                # Add to detailed breakdown
                for detail in resource_data.get("migrated_details", []):
                    detailed_report["detailed_breakdown"]["migrated"].append(
                        {
                            "project": project_name,
                            "resource_type": resource_type,
                            "source_id": detail["source_id"],
                            "dest_id": detail["dest_id"],
                            "name": detail["name"],
                        }
                    )

                for detail in resource_data.get("skipped_details", []):
                    detailed_report["detailed_breakdown"]["skipped"].append(
                        {
                            "project": project_name,
                            "resource_type": resource_type,
                            "source_id": detail["source_id"],
                            "dest_id": detail.get("dest_id"),
                            "name": detail["name"],
                            "skip_reason": detail["skip_reason"],
                        }
                    )

                for error in resource_data.get("errors", []):
                    detailed_report["detailed_breakdown"]["failed"].append(
                        {
                            "project": project_name,
                            "resource_type": resource_type,
                            "source_id": error.get("source_id"),
                            "name": error.get("name"),
                            "error": error.get("error"),
                        }
                    )

            detailed_report["projects"][project_name] = project_summary

        # Write JSON report to file
        with open(report_path, "w") as f:
            json.dump(detailed_report, f, indent=2, default=str)

        # Also create a human-readable summary
        summary_path = checkpoint_dir / "migration_summary.txt"
        self._write_human_readable_summary(detailed_report, summary_path)

        self._logger.info(
            "Generated detailed migration report",
            report_path=str(report_path),
            summary_path=str(summary_path),
            total_migrated=len(detailed_report["detailed_breakdown"]["migrated"]),
            total_skipped=len(detailed_report["detailed_breakdown"]["skipped"]),
            total_failed=len(detailed_report["detailed_breakdown"]["failed"]),
        )

        return report_path

    def _write_human_readable_summary(
        self, detailed_report: dict[str, Any], summary_path: Path
    ) -> None:
        """Write a human-readable summary of the migration.

        Args:
            detailed_report: The detailed report data.
            summary_path: Path to write the summary.
        """
        with open(summary_path, "w") as f:
            f.write("# Braintrust Migration Summary\n")
            f.write("=" * 50 + "\n\n")

            # Overall summary
            summary = detailed_report["migration_summary"]
            f.write(
                f"Migration Status: {'✅ SUCCESS' if summary['success'] else '❌ FAILED'}\n"
            )
            f.write(f"Start Time: {summary['start_time']}\n")
            f.write(f"End Time: {summary['end_time']}\n")
            f.write(f"Duration: {summary['duration_seconds']:.2f} seconds\n\n")

            f.write("## Overall Results\n")
            f.write(f"Projects: {summary['total_projects']}\n")
            f.write(f"Total Resources: {summary['total_resources']}\n")
            f.write(f"✅ Migrated: {summary['migrated_resources']}\n")
            f.write(f"⏭️  Skipped: {summary['skipped_resources']}\n")
            f.write(f"❌ Failed: {summary['failed_resources']}\n\n")

            # Organization resources breakdown
            org_data = detailed_report.get("organization_resources", {})
            if org_data and org_data.get("total_resources", 0) > 0:
                f.write("## Organization Resources\n")
                f.write(f"Resources: {org_data['total_resources']} total, ")
                f.write(f"{org_data['migrated_resources']} migrated, ")
                f.write(f"{org_data['skipped_resources']} skipped, ")
                f.write(f"{org_data['failed_resources']} failed\n")

                # Organization resource type breakdown
                for resource_type, counts in org_data["resources"].items():
                    if counts["total"] > 0:
                        f.write(f"  - {resource_type}: {counts['total']} total ")
                        f.write(
                            f"({counts['migrated']} migrated, {counts['skipped']} skipped, {counts['failed']} failed)\n"
                        )
                f.write("\n")

            # Project breakdown
            f.write("## Project Breakdown\n")
            for project_name, project_data in detailed_report["projects"].items():
                f.write(f"\n### {project_name}\n")
                f.write(f"Project ID: {project_data['project_id']}\n")
                f.write(f"Resources: {project_data['total_resources']} total, ")
                f.write(f"{project_data['migrated_resources']} migrated, ")
                f.write(f"{project_data['skipped_resources']} skipped, ")
                f.write(f"{project_data['failed_resources']} failed\n")

                # Resource type breakdown
                for resource_type, counts in project_data["resources"].items():
                    if counts["total"] > 0:
                        f.write(f"  - {resource_type}: {counts['total']} total ")
                        f.write(
                            f"({counts['migrated']} migrated, {counts['skipped']} skipped, {counts['failed']} failed)\n"
                        )

            # Skipped resources detail
            skipped = detailed_report["detailed_breakdown"]["skipped"]
            if skipped:
                f.write(f"\n## Skipped Resources ({len(skipped)} total)\n")

                # Group by skip reason
                skip_reasons = {}
                for item in skipped:
                    reason = item["skip_reason"]
                    if reason not in skip_reasons:
                        skip_reasons[reason] = []
                    skip_reasons[reason].append(item)

                for reason, items in skip_reasons.items():
                    f.write(
                        f"\n### {reason.replace('_', ' ').title()} ({len(items)} items)\n"
                    )
                    for item in items[:MAX_SUMMARY_ITEMS_PER_REASON]:
                        name_str = f" '{item['name']}'" if item["name"] else ""
                        f.write(
                            f"  - {item['resource_type']}: {item['source_id']}{name_str}\n"
                        )
                    if len(items) > MAX_SUMMARY_ITEMS_PER_REASON:
                        remaining = len(items) - MAX_SUMMARY_ITEMS_PER_REASON
                        f.write(f"  ... and {remaining} more\n")

            # Failed resources detail
            failed = detailed_report["detailed_breakdown"]["failed"]
            if failed:
                f.write(f"\n## Failed Resources ({len(failed)} total)\n")
                for item in failed:
                    name_str = f" '{item['name']}'" if item.get("name") else ""
                    f.write(
                        f"- {item['resource_type']}: {item['source_id']}{name_str}\n"
                    )
                    f.write(f"  Error: {item['error']}\n\n")

            f.write("\n## Files Generated\n")
            f.write("- Detailed JSON report: migration_report.json\n")
            f.write("- Human-readable summary: migration_summary.txt\n")
            f.write("- Checkpoint files: *_state.json\n")

    async def _migrate_organization_resources(
        self,
        source_client: BraintrustClient,
        dest_client: BraintrustClient,
        checkpoint_dir: Path,
        global_id_mappings: dict[str, str],
    ) -> dict[str, Any]:
        """Migrate organization-scoped resources once for the entire migration.

        Args:
            source_client: Source organization client.
            dest_client: Destination organization client.
            checkpoint_dir: Directory for storing checkpoints.
            global_id_mappings: Global ID mappings shared across all projects.

        Returns:
            Migration results for organization-scoped resources.
        """
        self._logger.info("Starting organization-scoped resource migration")

        # Create organization-specific checkpoint directory
        org_checkpoint_dir = checkpoint_dir / "organization"
        org_checkpoint_dir.mkdir(parents=True, exist_ok=True)

        org_results = {
            "resources": {},
            "total_resources": 0,
            "migrated_resources": 0,
            "skipped_resources": 0,
            "failed_resources": 0,
            "errors": [],
        }

        # Filter organization-scoped resources to migrate based on config
        org_resources_to_migrate = self._get_organization_resources_to_migrate()

        # Create shared dependency cache for efficiency
        shared_dependency_cache = {}

        # Migrate organization-scoped resources in dependency order
        for resource_name, migrator_class in self.ORGANIZATION_SCOPED_RESOURCES:
            if resource_name not in org_resources_to_migrate:
                self._logger.debug(f"Skipping {resource_name} (not in migration list)")
                continue

            try:
                self._logger.info(f"Migrating organization resource: {resource_name}")

                # Create migrator instance
                migrator = migrator_class(
                    source_client,
                    dest_client,
                    org_checkpoint_dir,
                    self.config.migration.batch_size,
                )

                # Organization-scoped resources don't have a destination project ID
                migrator.set_destination_project_id(None)

                # Share ID mappings from previous migrators
                migrator.update_id_mappings(global_id_mappings)

                # Pre-populate dependency mappings before migration
                await migrator.populate_dependency_mappings(
                    source_client,
                    dest_client,
                    None,  # No project_id for org-scoped resources
                    shared_dependency_cache,
                )

                # Perform migration (project_id=None for org-scoped)
                resource_results = await migrator.migrate_all(None)

                # Collect ID mappings from this migrator and add to global registry
                new_mappings = {
                    k: v
                    for k, v in migrator.state.id_mapping.items()
                    if k not in global_id_mappings  # Only add new mappings
                }
                global_id_mappings.update(migrator.state.id_mapping)

                if new_mappings:
                    self._logger.debug(
                        f"Added {len(new_mappings)} new ID mappings from {migrator_class.__name__}",
                        new_mappings_count=len(new_mappings),
                    )

                # Store results
                org_results["resources"][resource_name] = resource_results

                # Aggregate results
                org_results["total_resources"] += resource_results["total"]
                org_results["migrated_resources"] += resource_results["migrated"]
                org_results["skipped_resources"] += resource_results["skipped"]
                org_results["failed_resources"] += resource_results["failed"]
                org_results["errors"].extend(resource_results["errors"])

                self._logger.info(
                    f"Completed organization resource migration: {resource_name}",
                    total=resource_results["total"],
                    migrated=resource_results["migrated"],
                    skipped=resource_results["skipped"],
                    failed=resource_results["failed"],
                )

            except Exception as e:
                error_msg = (
                    f"Failed to migrate organization resource {resource_name}: {e}"
                )
                self._logger.error(error_msg)
                org_results["errors"].append(
                    {
                        "resource_type": resource_name,
                        "error": error_msg,
                    }
                )

        self._logger.info(
            "Completed organization-scoped resource migration",
            total_resources=org_results["total_resources"],
            migrated=org_results["migrated_resources"],
            skipped=org_results["skipped_resources"],
            failed=org_results["failed_resources"],
        )

        return org_results
