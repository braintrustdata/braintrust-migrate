"""Project automation migration functionality."""

from .base import ResourceMigrator


class ProjectAutomationMigrator(ResourceMigrator[dict]):
    """Migrator for project automations.

    Handles migration of automation rules (log alerts, BTQL exports, etc.)
    that are scoped to a project.
    """

    @property
    def resource_name(self) -> str:
        """Return the name of this resource type."""
        return "ProjectAutomations"

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        """List all project automations from the source organization.

        Args:
            project_id: Optional project ID to filter automations.

        Returns:
            List of project automation dicts from the source organization.
        """
        try:
            # The GET /v1/project_automation endpoint does not support
            # server-side project_id filtering, so we filter client-side.
            return await self._list_resources_with_client(
                self.source_client,
                "project_automations",
                project_id,
                client_side_filter_field="project_id",
            )
        except Exception as e:
            self._logger.error(
                "Failed to list source project automations", error=str(e)
            )
            raise

    async def migrate_resource(self, resource: dict) -> str:
        """Migrate a single project automation to the destination.

        Args:
            resource: The project automation dict to migrate.

        Returns:
            The ID of the created project automation in the destination.

        Raises:
            ValueError: If no destination project mapping is found.
            Exception: If the API call fails.
        """
        source_project_id = resource.get("project_id")
        dest_project_id = (
            self.state.id_mapping.get(source_project_id) if source_project_id else None
        )
        if not dest_project_id:
            dest_project_id = self.dest_project_id

        if not dest_project_id:
            error_msg = (
                f"No destination project mapping found for project automation "
                f"'{resource.get('name')}' (source project: {source_project_id})"
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        try:
            create_data = self.serialize_resource_for_insert(resource)
            create_data["project_id"] = dest_project_id

            self._logger.info(
                "Creating project automation in destination",
                automation_name=resource.get("name"),
                dest_project_id=dest_project_id,
            )

            response = await self.dest_client.with_retry(
                "create_project_automation",
                lambda create_data=create_data: self.dest_client.raw_request(
                    "POST",
                    "/v1/project_automation",
                    json=create_data,
                ),
            )

            dest_automation_id = response.get("id")
            if not dest_automation_id:
                raise ValueError(
                    f"No ID returned when creating project automation "
                    f"'{resource.get('name')}'"
                )

            self._logger.info(
                "Successfully migrated project automation",
                automation_name=resource.get("name"),
                source_id=resource.get("id"),
                dest_id=dest_automation_id,
            )

            return dest_automation_id

        except Exception as e:
            error_msg = (
                f"Failed to migrate project automation '{resource.get('name')}': {e}"
            )
            self._logger.error(error_msg, source_id=resource.get("id"))
            raise Exception(error_msg) from e

    async def get_dependencies(self, resource: dict) -> list[str]:
        """Get dependencies for a project automation.

        Project automations only depend on the project itself, which is
        migrated before any project-scoped resources.

        Args:
            resource: The project automation dict.

        Returns:
            Empty list — no cross-resource dependencies.
        """
        return []
