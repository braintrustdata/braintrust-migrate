from __future__ import annotations

from pathlib import Path
from typing import Any

from braintrust_migrate.resources.base import ResourceMigrator


class _DummyMigrator(ResourceMigrator[Any]):
    @property
    def resource_name(self) -> str:
        return "DummyResources"

    @property
    def allowed_fields_for_insert(self) -> set[str] | None:  # type: ignore[override]
        # Avoid coupling this unit test to the OpenAPI spec.
        return None

    async def list_source_resources(self, project_id: str | None = None) -> list[Any]:
        return []

    async def migrate_resource(self, resource: Any) -> str:
        raise NotImplementedError


class _ToDictKwargsBroken:
    id = "fn-123"

    def to_dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        # Simulate the real-world failure we saw with some Braintrust objects:
        # "'MockValSer' object cannot be converted to 'SchemaSerializer'".
        if kwargs:
            raise RuntimeError(
                "'MockValSer' object cannot be converted to 'SchemaSerializer'"
            )
        return {"id": self.id, "name": "ok"}


class _ToDictBrokenModelDumpOk:
    id = "fn-456"

    def to_dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("to_dict broken")

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"id": self.id, "name": "ok"}


def test_serialize_resource_falls_back_to_to_dict_without_kwargs(
    mock_source_client: Any, mock_dest_client: Any, tmp_path: Path
) -> None:
    migrator = _DummyMigrator(mock_source_client, mock_dest_client, tmp_path)
    out = migrator.serialize_resource_for_insert(_ToDictKwargsBroken())
    assert out["id"] == "fn-123"
    assert out["name"] == "ok"


def test_serialize_resource_falls_back_to_model_dump(
    mock_source_client: Any, mock_dest_client: Any, tmp_path: Path
) -> None:
    migrator = _DummyMigrator(mock_source_client, mock_dest_client, tmp_path)
    out = migrator.serialize_resource_for_insert(_ToDictBrokenModelDumpOk())
    assert out["id"] == "fn-456"
    assert out["name"] == "ok"
