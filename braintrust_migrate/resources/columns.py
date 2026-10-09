"""Custom Logs column migrator for Braintrust migration tool."""

from typing import Any

from braintrust_migrate.resources.base import ResourceMigrator


class ColumnMigrator(ResourceMigrator[dict]):
    """Migrate custom column names and expressions for project Logs.

    Saved view configuration is migrated separately by ViewMigrator.
    """

    @property
    def resource_name(self) -> str:
        return "Columns"

    @property
    def allowed_fields_for_insert(self) -> set[str]:
        # The bundled OpenAPI specification does not include /v1/column.
        return {"object_type", "object_id", "subtype", "variant", "name", "expr"}

    async def _list_columns(self, client: Any, project_id: str) -> list[dict]:
        """Read the complete scoped list; this endpoint does not paginate."""
        response = await client.with_retry(
            "list_columns",
            lambda: client.raw_request(
                "GET",
                "/v1/column",
                params={
                    "object_type": "project",
                    "object_id": project_id,
                    "subtype": "project_log",
                    "variant": "project_log",
                },
            ),
        )
        columns = response.get("objects")
        if not isinstance(columns, list):
            raise ValueError("Invalid /v1/column response: expected objects list")
        names: set[str] = set()
        for column in columns:
            if (
                not isinstance(column, dict)
                or column.get("object_type") != "project"
                or column.get("object_id") != project_id
                or column.get("subtype") != "project_log"
                or column.get("variant") != "project_log"
                or not isinstance(column.get("id"), str)
                or not column["id"]
                or not isinstance(column.get("name"), str)
                or not isinstance(column.get("expr"), str)
            ):
                raise ValueError(
                    "Invalid custom Logs column or unexpected project scope"
                )
            if column["name"] in names:
                raise ValueError(f"Duplicate custom column name: {column['name']!r}")
            names.add(column["name"])
        return columns

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        """List custom Logs columns belonging to the source project."""
        if not project_id:
            raise ValueError("A source project ID is required for columns")
        return await self._list_columns(self.source_client, project_id)

    async def prepare_resources(self, resources: list[dict]) -> None:
        """Check every name for conflicts before creating this project's columns."""
        if not self.dest_project_id:
            raise ValueError("A destination project ID is required for columns")
        destination = {
            column["name"]: column
            for column in await self._list_columns(
                self.dest_client, self.dest_project_id
            )
        }
        conflicts = [
            column["name"]
            for column in resources
            if column["name"] in destination
            and destination[column["name"]]["expr"] != column["expr"]
        ]
        if conflicts:
            raise ValueError(f"Custom column expression conflicts: {conflicts!r}")

        # Reconcile checkpoints against the actual destination on every run.
        for column in resources:
            source_id = column["id"]
            existing = destination.get(column["name"])
            if existing:
                self.state.id_mapping[source_id] = existing["id"]
                self.state.completed_ids.add(source_id)
                self.state.failed_ids.discard(source_id)
            else:
                self.state.id_mapping.pop(source_id, None)
                self.state.completed_ids.discard(source_id)
        self._columns_to_verify = resources

    async def migrate_resource(self, resource: dict) -> str:
        """Create a column using the mapped destination project ID."""
        if not self.dest_project_id:
            raise ValueError("A destination project ID is required for columns")
        column_data = self.serialize_resource_for_insert(resource)
        column_data["object_id"] = self.dest_project_id
        response = await self.dest_client.with_retry(
            "create_column",
            lambda: self.dest_client.raw_request(
                "POST", "/v1/column", json=column_data
            ),
        )
        dest_id = response.get("id")
        if not isinstance(dest_id, str) or not dest_id:
            raise ValueError(
                f"No ID returned when creating column {resource.get('name')}"
            )
        # POST may return an existing column if another writer created it meanwhile.
        if any(response.get(key) != value for key, value in column_data.items()):
            raise ValueError(f"Created column does not match {resource.get('name')!r}")
        self._logger.info(
            "Successfully migrated column",
            source_id=resource.get("id"),
            dest_id=dest_id,
            name=resource.get("name"),
        )
        return dest_id

    async def migrate_all(
        self, project_id: str | None = None, max_concurrent: int | None = None
    ) -> dict[str, Any]:
        """Migrate columns and verify the definitions by reading the destination."""
        summary = await super().migrate_all(project_id, max_concurrent)
        if not self.dest_project_id:
            raise ValueError("A destination project ID is required for columns")
        destination = {
            column["name"]: column
            for column in await self._list_columns(
                self.dest_client, self.dest_project_id
            )
        }
        mismatches = []
        for column in self._columns_to_verify:
            existing = destination.get(column["name"])
            if not existing or existing["expr"] != column["expr"]:
                mismatches.append(column["name"])
                self.state.id_mapping.pop(column["id"], None)
                self.record_failure(column["id"], "Destination verification failed")
        self._save_state()
        if mismatches:
            raise ValueError(f"Custom column verification failed: {mismatches!r}")
        self._logger.info(
            "Verified custom Logs columns", total=len(self._columns_to_verify)
        )
        return summary
