"""Prompt migrator for Braintrust migration tool."""

import copy

from braintrust_migrate.resources.base import ResourceMigrator


class PromptMigrator(ResourceMigrator[dict]):
    """Migrator for Braintrust prompts.

    Uses raw API requests instead of SDK to avoid model dependencies.
    """

    @property
    def resource_name(self) -> str:
        """Human-readable name for this resource type."""
        return "Prompts"

    async def get_dependencies(self, resource: dict) -> list[str]:
        """Get list of resource IDs that this prompt depends on.

        Prompts can depend on:
        1. Functions/tools via prompt_data.tool_functions
        2. Other prompts via prompt_data.origin.prompt_id

        Args:
            resource: Prompt dict to get dependencies for.

        Returns:
            List of resource IDs this prompt depends on.
        """
        dependencies = []

        # Check prompt_data for dependencies
        prompt_data = resource.get("prompt_data")
        if prompt_data:
            # Not checking for tool_functions because they are already handled by the
            # function migrator

            # Check for prompt origin dependencies
            origin = prompt_data.get("origin")
            if origin:
                prompt_id = origin.get("prompt_id")
                if prompt_id:
                    dependencies.append(prompt_id)
                    self._logger.debug(
                        "Found prompt dependency",
                        prompt_id=resource.get("id"),
                        prompt_name=resource.get("name"),
                        origin_prompt_id=prompt_id,
                    )

        return dependencies

    async def get_dependency_types(self) -> list[str]:
        """Get list of resource types that prompts might depend on.

        Returns:
            List of resource type names that prompts can depend on.
        """
        return ["functions", "prompts"]

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        """List all prompts from the source organization using raw API.

        Args:
            project_id: Optional project ID to filter prompts.

        Returns:
            List of prompt dicts from the source organization.
        """
        try:
            # Use base class helper method
            return await self._list_resources_with_client(
                self.source_client, "prompts", project_id
            )

        except Exception as e:
            self._logger.error("Failed to list source prompts", error=str(e))
            raise

    def _resolve_prompt_data_dependencies(self, prompt_data: dict) -> dict:
        """Resolve dependencies within prompt_data and return updated version.

        Args:
            prompt_data: Original prompt data.

        Returns:
            Updated prompt data with resolved dependencies.
        """
        if not prompt_data:
            return prompt_data

        # Create a deep copy to avoid modifying the original
        resolved_data = copy.deepcopy(prompt_data)

        # Resolve tool_functions dependencies using base class utility
        if resolved_data.get("tool_functions"):
            resolved_tool_functions = []
            for tool_func in resolved_data["tool_functions"]:
                # Use base class generic function resolver for all function types
                resolved_function = self._resolve_function_reference_generic(tool_func)
                if resolved_function:
                    resolved_tool_functions.append(resolved_function)
                elif (
                    isinstance(tool_func, dict) and tool_func.get("type") == "function"
                ):
                    # Only warn for function types that failed to resolve
                    self._logger.warning(
                        "Could not resolve tool function dependency",
                        source_function_id=tool_func.get("id", "unknown"),
                    )
                    # Skip this tool function rather than break the prompt
                else:
                    # Keep non-function types (like global functions) as-is
                    resolved_tool_functions.append(tool_func)

            resolved_data["tool_functions"] = resolved_tool_functions

        # Resolve origin prompt dependencies using simple ID mapping
        if resolved_data.get("origin") and isinstance(resolved_data["origin"], dict):
            origin = resolved_data["origin"]
            if "prompt_id" in origin:
                dest_prompt_id = self.state.id_mapping.get(origin["prompt_id"])
                if dest_prompt_id:
                    resolved_data["origin"]["prompt_id"] = dest_prompt_id
                    # Also update project_id if present
                    if "project_id" in origin:
                        resolved_data["origin"]["project_id"] = self.dest_project_id
                else:
                    self._logger.warning(
                        "Could not resolve prompt origin dependency",
                        source_prompt_id=origin["prompt_id"],
                    )
                    # Remove the origin to avoid broken references
                    del resolved_data["origin"]

        return resolved_data

    async def migrate_resource(self, resource: dict) -> str:
        """Migrate a single prompt from source to destination using raw API.

        Args:
            resource: Source prompt dict to migrate.

        Returns:
            ID of the created prompt in destination.

        Raises:
            Exception: If migration fails.
        """
        self._logger.info(
            "Migrating prompt",
            source_id=resource.get("id"),
            name=resource.get("name"),
            slug=resource.get("slug"),
            project_id=resource.get("project_id"),
        )

        # Create prompt in destination using base class serialization
        create_params = self.serialize_resource_for_insert(resource)

        # Override the project_id to use destination project
        create_params["project_id"] = self.dest_project_id

        # Handle prompt_data with dependency resolution
        prompt_data = resource.get("prompt_data")
        if prompt_data:
            # Resolve dependencies in prompt_data
            resolved_prompt_data = self._resolve_prompt_data_dependencies(prompt_data)
            create_params["prompt_data"] = resolved_prompt_data

        # Create prompt using raw API
        response = await self.dest_client.with_retry(
            "create_prompt",
            lambda create_params=create_params: self.dest_client.raw_request(
                "POST",
                "/v1/prompt",
                json=create_params,
            ),
        )

        dest_prompt_id = response.get("id")
        if not dest_prompt_id:
            raise ValueError(
                f"No ID returned when creating prompt {resource.get('name')}"
            )

        self._logger.info(
            "Created prompt in destination",
            source_id=resource.get("id"),
            dest_id=dest_prompt_id,
            name=resource.get("name"),
            slug=resource.get("slug"),
        )

        return dest_prompt_id
