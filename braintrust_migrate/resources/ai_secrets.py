"""AI Secrets migrator for Braintrust migration tool."""

from braintrust_migrate.resources.base import ResourceMigrator


class AISecretMigrator(ResourceMigrator[dict]):
    """Migrator for Braintrust AI Secrets (AI Provider credentials).

    AI Secrets are organization-scoped and store credentials for AI providers
    like OpenAI, Anthropic, Google, etc. They have no dependencies on other
    resources and should be migrated early in the process.

    Note: Secret values are not exposed in API responses for security reasons.
    During migration, only the secret metadata and configuration are copied,
    not the actual secret values.

    Uses raw API requests instead of SDK to avoid model dependencies.
    """

    @property
    def resource_name(self) -> str:
        """Human-readable name for this resource type."""
        return "AISecrets"

    async def get_dependencies(self, resource: dict) -> list[str]:
        """Get list of resource IDs that this AI secret depends on.

        AI Secrets have no dependencies on other resources.

        Args:
            resource: AI Secret dict to get dependencies for.

        Returns:
            Empty list - AI Secrets have no dependencies.
        """
        return []

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        """List all AI secrets from the source organization using raw API.

        Args:
            project_id: Not used for AI secrets (they are org-scoped).

        Returns:
            List of AI secret dicts from the source organization.
        """
        try:
            # AI secrets are organization-scoped, not project-scoped
            # Use base class helper but without project_id
            return await self._list_resources_with_client(
                self.source_client, "ai_secrets", project_id=None
            )

        except Exception as e:
            self._logger.error("Failed to list source AI secrets", error=str(e))
            raise

    async def migrate_resource(self, resource: dict) -> str:
        """Migrate a single AI secret from source to destination using raw API.

        Args:
            resource: Source AI secret dict to migrate.

        Returns:
            ID of the created AI secret in destination.

        Raises:
            Exception: If migration fails.
        """
        self._logger.info(
            "Migrating AI secret",
            source_id=resource.get("id"),
            name=resource.get("name"),
            type=resource.get("type"),
            org_id=resource.get("org_id"),
        )

        # Create AI secret in destination using base class serialization
        create_params = self.serialize_resource_for_insert(resource)

        # Note: We intentionally do NOT copy the secret value itself
        # for security reasons. The actual secret values must be manually
        # configured in the destination organization by administrators.
        self._logger.warning(
            "AI secret metadata copied - secret value must be manually configured in destination",
            ai_secret_id=resource.get("id"),
            ai_secret_name=resource.get("name"),
            ai_secret_type=resource.get("type"),
        )

        # Create AI secret using raw API
        response = await self.dest_client.with_retry(
            "create_ai_secret",
            lambda create_params=create_params: self.dest_client.raw_request(
                "POST",
                "/v1/ai_secret",
                json=create_params,
            ),
        )

        dest_ai_secret_id = response.get("id")
        if not dest_ai_secret_id:
            raise ValueError(
                f"No ID returned when creating AI secret {resource.get('name')}"
            )

        self._logger.info(
            "Created AI secret in destination",
            source_id=resource.get("id"),
            dest_id=dest_ai_secret_id,
            name=resource.get("name"),
            type=resource.get("type"),
        )

        return dest_ai_secret_id
