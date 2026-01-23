"""Project score migration functionality."""

from .base import ResourceMigrator


class ProjectScoreMigrator(ResourceMigrator[dict]):
    """Migrator for project scores.

    Uses raw API requests instead of SDK to avoid model dependencies.
    """

    @property
    def resource_name(self) -> str:
        """Return the name of this resource type."""
        return "ProjectScores"

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        """List all project scores from the source organization using raw API.

        Args:
            project_id: Optional project ID to filter by

        Returns:
            List of project score dicts
        """
        self._logger.info("Listing project scores from source", project_id=project_id)

        try:
            return await self._list_resources_with_client(
                self.source_client,
                "project_scores",
                project_id,
                client_side_filter_field="project_id",
            )

        except Exception as e:
            self._logger.error(
                "Failed to list project scores", error=str(e), project_id=project_id
            )
            raise

    async def migrate_resource(self, resource: dict) -> str:
        """Migrate a single project score to the destination using raw API.

        Args:
            resource: The project score dict to migrate

        Returns:
            The ID of the migrated project score

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
            error_msg = f"No destination project mapping found for project score '{resource.get('name')}' (source project: {source_project_id})"
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        try:
            # Create the project score in the destination
            create_data = self.serialize_resource_for_insert(resource)

            # Override the project_id with the destination project ID
            create_data["project_id"] = dest_project_id

            # Handle config with potential function dependencies
            config = resource.get("config")
            if config is not None:
                resolved_config = await self._resolve_config_dependencies(config)
                create_data["config"] = resolved_config

            self._logger.info(
                "Creating project score in destination",
                score_name=resource.get("name"),
                score_type=resource.get("score_type"),
                dest_project_id=dest_project_id,
            )

            # Create project score using raw API
            response = await self.dest_client.with_retry(
                "create_project_score",
                lambda create_data=create_data: self.dest_client.raw_request(
                    "POST",
                    "/v1/project_score",
                    json=create_data,
                ),
            )

            dest_score_id = response.get("id")
            if not dest_score_id:
                raise ValueError(
                    f"No ID returned when creating project score {resource.get('name')}"
                )

            self._logger.info(
                "Successfully migrated project score",
                score_name=resource.get("name"),
                source_id=resource.get("id"),
                dest_id=dest_score_id,
            )

            return dest_score_id

        except Exception as e:
            error_msg = f"Failed to migrate project score '{resource.get('name')}': {e}"
            self._logger.error(error_msg, source_id=resource.get("id"))
            raise Exception(error_msg) from e

    async def _resolve_config_dependencies(self, config: dict) -> dict:
        """Resolve function dependencies in project score config.

        Args:
            config: The project score config dict

        Returns:
            Config with resolved function IDs
        """
        if not config:
            return config

        # Config is already a dict from raw API
        config_dict = dict(config)

        # Handle online scoring config with dependency resolution
        if config_dict.get("online"):
            online_config = config_dict["online"]

            # Resolve scorer function dependencies if present
            if online_config.get("scorers"):
                resolved_scorers = []
                for scorer in online_config["scorers"]:
                    resolved_scorer = self._resolve_function_reference_generic(scorer)
                    if resolved_scorer:
                        resolved_scorers.append(resolved_scorer)
                    else:
                        self._logger.warning(
                            "Failed to resolve scorer function reference",
                            scorer=scorer,
                        )
                online_config["scorers"] = resolved_scorers
                config_dict["online"] = online_config

        return config_dict

    async def get_dependencies(self, resource: dict) -> list[str]:
        """Get the dependencies for a project score.

        Project scores depend on:
        1. Projects (migrated first)
        2. Functions (for online scoring config)

        Args:
            resource: The project score dict to get dependencies for

        Returns:
            List of dependency IDs
        """
        project_id = resource.get("project_id")
        dependencies = [project_id] if project_id else []

        # Check for function dependencies in config
        config = resource.get("config")
        if config:
            online_config = config.get("online")
            if online_config:
                scorers = online_config.get("scorers")
                if scorers:
                    for scorer in scorers:
                        if (
                            isinstance(scorer, dict)
                            and scorer.get("type") == "function"
                        ):
                            scorer_id = scorer.get("id")
                            if scorer_id:
                                dependencies.append(scorer_id)

        return dependencies
