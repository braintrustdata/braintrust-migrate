"""SpanIframe migrator for Braintrust migration tool."""

from braintrust_migrate.resources.base import ResourceMigrator


class SpanIframeMigrator(ResourceMigrator[dict]):
    """Migrator for Braintrust span iframes.

    Handles migration of:
    - Span iframe metadata (name, description, url, etc.)
    - Iframe configuration (post_message settings)

    SpanIframes are project-scoped resources with no dependencies on other resources.
    They contain URLs for embedding project viewers in iframes.

    Uses raw API requests instead of SDK to avoid model dependencies.
    """

    @property
    def resource_name(self) -> str:
        """Human-readable name for this resource type."""
        return "SpanIFrames"

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        """List all span iframes from the source organization using raw API.

        Args:
            project_id: Optional project ID to filter span iframes.

        Returns:
            List of span iframe dicts from the source organization.
        """
        try:
            # Use base class helper method with client-side filtering
            return await self._list_resources_with_client(
                self.source_client,
                "span_iframes",
                project_id,
                client_side_filter_field="project_id",
            )

        except Exception as e:
            self._logger.error("Failed to list source span iframes", error=str(e))
            raise

    async def migrate_resource(self, resource: dict) -> str:
        """Migrate a single span iframe from source to destination using raw API.

        Args:
            resource: Source span iframe dict to migrate.

        Returns:
            ID of the created span iframe in destination.

        Raises:
            Exception: If migration fails.
        """
        self._logger.info(
            "Migrating span iframe",
            source_id=resource.get("id"),
            name=resource.get("name"),
            project_id=resource.get("project_id"),
        )

        # Create span iframe in destination using base class serialization
        create_params = self.serialize_resource_for_insert(resource)

        # Override the project_id to use destination project
        create_params["project_id"] = self.dest_project_id

        # Create span iframe using raw API
        response = await self.dest_client.with_retry(
            "create_span_iframe",
            lambda create_params=create_params: self.dest_client.raw_request(
                "POST",
                "/v1/span_iframe",
                json=create_params,
            ),
        )

        dest_span_iframe_id = response.get("id")
        if not dest_span_iframe_id:
            raise ValueError(
                f"No ID returned when creating span iframe {resource.get('name')}"
            )

        self._logger.info(
            "Created span iframe in destination",
            source_id=resource.get("id"),
            dest_id=dest_span_iframe_id,
            name=resource.get("name"),
        )

        return dest_span_iframe_id

    async def get_dependencies(self, resource: dict) -> list[str]:
        """Get dependencies for a span iframe.

        SpanIframes only depend on projects, which are migrated first.
        No other resource dependencies.

        Args:
            resource: SpanIFrame dict to analyze.

        Returns:
            Empty list (no dependencies on other migratable resources).
        """
        return []
