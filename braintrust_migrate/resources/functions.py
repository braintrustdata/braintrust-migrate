"""Function migrator for Braintrust migration tool."""

from braintrust_migrate.resources.base import ResourceMigrator


class FunctionMigrator(ResourceMigrator[dict]):
    """Migrator for Braintrust functions (including tools, scorers, tasks, and LLMs).

    Uses raw API requests instead of SDK to avoid Pydantic model mismatches.
    """

    @property
    def resource_name(self) -> str:
        """Human-readable name for this resource type."""
        return "Functions"

    async def get_dependencies(self, resource: dict) -> list[str]:
        """Get list of resource IDs that this function depends on.

        Functions can depend on other migratable resources via the origin field.

        Args:
            resource: Function dict to get dependencies for.

        Returns:
            List of resource IDs this function depends on.
        """
        dependencies = []

        # Check if function has an origin that references a migratable resource
        origin = resource.get("origin")
        if origin and isinstance(origin, dict):
            object_type = origin.get("object_type")
            object_id = origin.get("object_id")
            if (
                object_type in {"prompt", "dataset", "experiment", "project"}
                and object_id
            ):
                dependencies.append(object_id)
                self._logger.debug(
                    "Found origin dependency",
                    function_id=resource.get("id"),
                    function_name=resource.get("name"),
                    origin_type=object_type,
                    origin_id=object_id,
                )

        return dependencies

    async def get_dependency_types(self) -> list[str]:
        """Get list of resource types that functions might depend on.

        Returns:
            List of resource type names that functions can depend on.
        """
        return ["prompts", "datasets", "experiments"]

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        """List all functions from the source organization using raw API.

        Args:
            project_id: Optional project ID to filter functions.

        Returns:
            List of function dicts from the source organization.
        """
        try:
            # Use base class helper method
            return await self._list_resources_with_client(
                self.source_client, "functions", project_id
            )

        except Exception as e:
            self._logger.error("Failed to list source functions", error=str(e))
            raise

    async def migrate_resource(self, resource: dict) -> str:
        """Migrate a single function from source to destination using raw API.

        Args:
            resource: Source function dict to migrate.

        Returns:
            ID of the created function in destination.

        Raises:
            Exception: If migration fails.
        """
        self._logger.info(
            "Migrating function",
            source_id=resource.get("id"),
            name=resource.get("name"),
            slug=resource.get("slug"),
            project_id=resource.get("project_id"),
            function_type=resource.get("function_type"),
        )

        # Use base class serialization (filters via OpenAPI schema)
        create_params = self.serialize_resource_for_insert(resource)

        # Override the project_id to use destination project
        create_params["project_id"] = self.dest_project_id

        # Resolve origin dependencies if present
        origin = resource.get("origin")
        if origin and isinstance(origin, dict):
            object_type = origin.get("object_type")
            object_id = origin.get("object_id")

            if (
                object_type in {"prompt", "dataset", "experiment", "project"}
                and object_id
            ):
                # Resolve dependency to destination ID
                if object_type == "project":
                    # Use destination project ID
                    dest_object_id = self.dest_project_id
                else:
                    # Map origin object_type to resource type for API calls
                    resource_type_mapping = {
                        "prompt": "prompts",
                        "dataset": "datasets",
                        "experiment": "experiments",
                    }
                    resource_type = resource_type_mapping.get(object_type)

                    if resource_type:
                        # Ensure dependency mapping exists, populate if necessary
                        dest_object_id = await self.ensure_dependency_mapping(
                            resource_type, object_id, project_id=None
                        )
                    else:
                        self._logger.warning(
                            "Unknown origin object_type not in resource mapping - "
                            "this may indicate a new dependency type that needs support",
                            function_id=resource.get("id"),
                            function_name=resource.get("name"),
                            unknown_object_type=object_type,
                            object_id=object_id,
                        )
                        dest_object_id = None

                if dest_object_id:
                    # Update the origin object_id in create_params
                    if "origin" not in create_params:
                        create_params["origin"] = origin.copy()
                    create_params["origin"]["object_id"] = dest_object_id
                else:
                    self._logger.warning(
                        "Could not resolve origin dependency - referenced resource may not have been migrated",
                        function_id=resource.get("id"),
                        function_name=resource.get("name"),
                        origin_type=object_type,
                        source_object_id=object_id,
                    )
                    # Remove origin field to avoid broken references
                    create_params.pop("origin", None)

        # Create function using raw API
        response = await self.dest_client.with_retry(
            "create_function",
            lambda: self.dest_client.raw_request(
                "POST",
                "/v1/function",
                json=create_params,
            ),
        )

        dest_id = response.get("id")
        if not dest_id:
            raise ValueError(
                f"No ID returned when creating function {resource.get('name')}"
            )

        self._logger.info(
            "Created function in destination",
            source_id=resource.get("id"),
            dest_id=dest_id,
            name=resource.get("name"),
            slug=resource.get("slug"),
            function_type=response.get("function_type"),
        )

        return dest_id
