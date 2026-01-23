"""Unit tests for ViewMigrator dependency handling."""

from unittest.mock import AsyncMock

import pytest

from braintrust_migrate.resources.views import ViewMigrator


@pytest.fixture
def project_view():
    """Create a view attached to a project."""
    return {
        "id": "view-proj-123",
        "name": "Project View",
        "object_id": "project-456",
        "object_type": "project",
        "view_type": "table",
        "view_data": {"columns": ["name", "id"]},
    }


@pytest.fixture
def experiment_view():
    """Create a view attached to an experiment."""
    return {
        "id": "view-exp-123",
        "name": "Experiment View",
        "object_id": "exp-789",
        "object_type": "experiment",
        "view_type": "table",
        "view_data": {"columns": ["score", "output"]},
    }


@pytest.fixture
def dataset_view():
    """Create a view attached to a dataset."""
    return {
        "id": "view-ds-123",
        "name": "Dataset View",
        "object_id": "dataset-abc",
        "object_type": "dataset",
        "view_type": "table",
        "view_data": {"columns": ["input", "expected"]},
    }


@pytest.fixture
def unknown_type_view():
    """Create a view with unknown object type."""
    return {
        "id": "view-unknown-123",
        "name": "Unknown View",
        "object_id": "unknown-xyz",
        "object_type": "unknown_type",
        "view_type": "table",
        "view_data": {},
    }


@pytest.mark.asyncio
class TestViewDependencies:
    """Test ViewMigrator dependency resolution."""

    async def test_get_dependencies_project_view(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        project_view,
    ):
        """Test project views have no extra dependencies."""
        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        deps = await migrator.get_dependencies(project_view)

        assert deps == []

    async def test_get_dependencies_experiment_view(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        experiment_view,
    ):
        """Test experiment views depend on the experiment."""
        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        deps = await migrator.get_dependencies(experiment_view)

        assert deps == ["exp-789"]

    async def test_get_dependencies_dataset_view(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        dataset_view,
    ):
        """Test dataset views depend on the dataset."""
        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        deps = await migrator.get_dependencies(dataset_view)

        assert deps == ["dataset-abc"]

    async def test_get_dependencies_unknown_object_view(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        unknown_type_view,
    ):
        """Test views with unknown types add object_id as dependency."""
        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        deps = await migrator.get_dependencies(unknown_type_view)

        assert deps == ["unknown-xyz"]

    async def test_resolve_object_id_project(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        project_view,
    ):
        """Test resolving object_id for project views."""
        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        result = migrator._resolve_object_id(project_view)

        assert result == "dest-project-999"

    async def test_resolve_object_id_experiment_resolved(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        experiment_view,
    ):
        """Test resolving object_id for experiment views with mapping."""
        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.state.id_mapping["exp-789"] = "dest-exp-789"

        result = migrator._resolve_object_id(experiment_view)

        assert result == "dest-exp-789"

    async def test_resolve_object_id_experiment_unresolved(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        experiment_view,
    ):
        """Test resolving object_id for experiment views without mapping (fallback)."""
        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        # No mapping added

        result = migrator._resolve_object_id(experiment_view)

        # Falls back to source ID
        assert result == "exp-789"

    async def test_resolve_object_id_dataset_resolved(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        dataset_view,
    ):
        """Test resolving object_id for dataset views with mapping."""
        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.state.id_mapping["dataset-abc"] = "dest-dataset-abc"

        result = migrator._resolve_object_id(dataset_view)

        assert result == "dest-dataset-abc"

    async def test_resolve_object_id_unknown_type(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        unknown_type_view,
    ):
        """Test resolving object_id for unknown types (uses source ID)."""
        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        result = migrator._resolve_object_id(unknown_type_view)

        assert result == "unknown-xyz"

    async def test_migrate_resource_project_view(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        project_view,
    ):
        """Test migrating a project view."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-view-proj", "name": "Project View"}
        )

        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        result = await migrator.migrate_resource(project_view)

        assert result == "new-view-proj"

    async def test_migrate_resource_experiment_view_resolved(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        experiment_view,
    ):
        """Test migrating an experiment view with resolved dependency."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-view-exp", "name": "Experiment View"}
        )

        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.state.id_mapping["exp-789"] = "dest-exp-789"

        result = await migrator.migrate_resource(experiment_view)

        assert result == "new-view-exp"

    async def test_migrate_resource_dataset_view_unresolved(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        dataset_view,
    ):
        """Test migrating a dataset view without resolved dependency (uses fallback)."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-view-ds", "name": "Dataset View"}
        )

        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        # No mapping - will use source ID as fallback

        result = await migrator.migrate_resource(dataset_view)

        assert result == "new-view-ds"
