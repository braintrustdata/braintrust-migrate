"""View migrator for Braintrust migration tool."""

from braintrust_api.types import View

from braintrust_migrate.resources.base import ResourceMigrator


class ViewMigrator(ResourceMigrator[View]):
    """Migrator for Braintrust views.

    Views are saved table configurations that define how data is displayed
    in the Braintrust UI, including filters, sorting, column visibility,
    and other display options.
    """

    @property
    def resource_name(self) -> str:
        """Human-readable name for this resource type."""
        return "Views"

    async def get_dependencies(self, resource: View) -> list[str]:
        """Get list of resource IDs that this view depends on.

        Views can depend on other resources via object_id:
        - Projects (handled by dest_project_id mapping)
        - Experiments (need ID mapping)
        - Datasets (need ID mapping)
        - Other object types as they become available

        Args:
            resource: View to get dependencies for.

        Returns:
            List of resource IDs this view depends on.
        """
        dependencies = []

        # Check object_id dependency based on object_type
        if hasattr(resource, "object_id") and resource.object_id:
            object_type = getattr(resource, "object_type", None)

            # Only add as dependency if it's not a project (projects are handled separately)
            if object_type and object_type != "project":
                dependencies.append(resource.object_id)
                self._logger.debug(
                    "Found object dependency",
                    view_id=resource.id,
                    view_name=resource.name,
                    object_type=object_type,
                    object_id=resource.object_id,
                )

        return dependencies

    async def list_source_resources(self, project_id: str | None = None) -> list[View]:
        """List all views from the source organization.

        Uses OpenAPI parameter mapping to efficiently discover views.

        Args:
            project_id: Optional project ID to filter views.

        Returns:
            List of views from the source organization.
        """
        try:
            # Use OpenAPI parameter mapping for efficient discovery
            # The views API supports object_type and object_id parameters
            params = {}
            if project_id:
                # When filtering by project, use the object_id parameter
                params["object_id"] = project_id
                params["object_type"] = "project"

            # Use base class helper method for efficient API calls
            views = await self._list_resources_with_client(
                self.source_client, "views", additional_params=params
            )

            self._logger.info(
                f"Discovered {len(views)} views",
                total_views=len(views),
                project_id=project_id,
            )

            return views

        except Exception as e:
            self._logger.error("Failed to list source views", error=str(e))
            raise

    def _resolve_object_id(self, resource: View) -> str:
        """Resolve the object_id for the destination based on object_type.

        Args:
            resource: Source view to resolve object_id for.

        Returns:
            Resolved destination object_id.
        """
        object_type = getattr(resource, "object_type", None)
        source_object_id = resource.object_id

        # Handle different object types
        if object_type == "project":
            # Use destination project ID
            return self.dest_project_id or source_object_id
        elif object_type in ["experiment", "dataset"]:
            # Look up in ID mapping
            dest_object_id = self.state.id_mapping.get(source_object_id)
            if dest_object_id:
                self._logger.debug(
                    "Resolved object dependency for view",
                    view_id=resource.id,
                    object_type=object_type,
                    source_object_id=source_object_id,
                    dest_object_id=dest_object_id,
                )
                return dest_object_id
            else:
                self._logger.warning(
                    "Could not resolve object dependency for view",
                    view_id=resource.id,
                    object_type=object_type,
                    source_object_id=source_object_id,
                )
                # Return source ID as fallback
                return source_object_id
        else:
            # For unknown object types, use source ID as fallback
            self._logger.debug(
                "Unknown object type for view, using source object_id",
                view_id=resource.id,
                object_type=object_type,
                object_id=source_object_id,
            )
            return source_object_id

    async def migrate_resource(self, resource: View) -> str:
        """Migrate a single view to the destination.

        Args:
            resource: Source view to migrate.

        Returns:
            ID of the migrated view in the destination.
        """
        try:
            # Resolve the destination object_id
            dest_object_id = self._resolve_object_id(resource)

            view_data = self.serialize_resource_for_insert(resource)

            # Override the object_id with the resolved destination object_id
            view_data["object_id"] = dest_object_id

            # Create the view in the destination
            new_view = await self.dest_client.with_retry(
                "create_view",
                lambda: self.dest_client.client.views.create(**view_data),
            )

            self._logger.info(
                "Successfully migrated view",
                source_id=resource.id,
                dest_id=new_view.id,
                name=resource.name,
                view_type=resource.view_type,
                object_type=resource.object_type,
                source_object_id=resource.object_id,
                dest_object_id=dest_object_id,
            )

            return new_view.id

        except Exception as e:
            self._logger.error(
                "Failed to migrate view",
                view_id=resource.id,
                view_name=resource.name,
                error=str(e),
            )
            raise
