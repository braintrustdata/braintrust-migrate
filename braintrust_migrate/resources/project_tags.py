"""Project tag migration functionality."""

from .base import ResourceMigrator


class ProjectTagMigrator(ResourceMigrator[dict]):
    """Migrator for project tags.

    Uses raw API requests instead of SDK to avoid model dependencies.
    """

    @property
    def resource_name(self) -> str:
        """Return the name of this resource type."""
        return "ProjectTags"

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        """List all project tags from the source organization using raw API.

        Args:
            project_id: Optional project ID to filter project tags.

        Returns:
            List of project tag dicts from the source organization.
        """
        try:
            # Use base class helper method with client-side filtering
            return await self._list_resources_with_client(
                self.source_client,
                "project_tags",
                project_id,
                client_side_filter_field="project_id",
            )

        except Exception as e:
            self._logger.error("Failed to list source project tags", error=str(e))
            raise

    async def migrate_resource(self, resource: dict) -> str:
        """Migrate a single project tag to the destination using raw API.

        Args:
            resource: The project tag dict to migrate

        Returns:
            The ID of the migrated project tag

        Raises:
            Exception: If migration fails or no destination project mapping found
        """
        # Get the destination project ID
        # First try the ID mapping, then fall back to the configured dest_project_id
        source_project_id = resource.get("project_id")
        dest_project_id = (
            self.state.id_mapping.get(source_project_id) if source_project_id else None
        )
        if not dest_project_id:
            dest_project_id = self.dest_project_id

        if not dest_project_id:
            error_msg = f"No destination project mapping found for project tag '{resource.get('name')}' (source project: {source_project_id})"
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        try:
            # Create the project tag in the destination
            create_data = self.serialize_resource_for_insert(resource)

            # Override the project_id with the destination project ID
            create_data["project_id"] = dest_project_id

            self._logger.info(
                "Creating project tag in destination",
                tag_name=resource.get("name"),
                dest_project_id=dest_project_id,
            )

            # Create project tag using raw API
            response = await self.dest_client.with_retry(
                "create_project_tag",
                lambda create_data=create_data: self.dest_client.raw_request(
                    "POST",
                    "/v1/project_tag",
                    json=create_data,
                ),
            )

            dest_tag_id = response.get("id")
            if not dest_tag_id:
                raise ValueError(
                    f"No ID returned when creating project tag {resource.get('name')}"
                )

            self._logger.info(
                "Successfully migrated project tag",
                tag_name=resource.get("name"),
                source_id=resource.get("id"),
                dest_id=dest_tag_id,
            )

            return dest_tag_id

        except Exception as e:
            error_msg = f"Failed to migrate project tag '{resource.get('name')}': {e}"
            self._logger.error(error_msg, source_id=resource.get("id"))
            raise Exception(error_msg) from e

    async def get_dependencies(self, resource: dict) -> list[str]:
        """Get the dependencies for a project tag.

        Project tags only depend on projects, which are migrated first.

        Args:
            resource: The project tag dict to get dependencies for

        Returns:
            Empty list since project tags have no dependencies on other migratable resources
        """
        return []
