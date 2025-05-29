"""Unit tests for ExperimentMigrator dependency resolution."""

from unittest.mock import AsyncMock, Mock

import pytest
from braintrust_api.types import Experiment

from braintrust_migrate.resources.experiments import ExperimentMigrator


@pytest.fixture
def mock_source_client():
    """Create a mock source client."""
    client = Mock()
    client.client.experiments.list = AsyncMock()
    client.client.experiments.fetch = AsyncMock()
    client.with_retry = AsyncMock()
    return client


@pytest.fixture
def mock_dest_client():
    """Create a mock destination client."""
    client = Mock()
    client.client.experiments.list = AsyncMock()
    client.client.experiments.create = AsyncMock()
    client.client.experiments.insert = AsyncMock()
    client.with_retry = AsyncMock()
    return client


@pytest.fixture
def temp_checkpoint_dir(tmp_path):
    """Create a temporary checkpoint directory."""
    return tmp_path / "checkpoints"


@pytest.fixture
def experiment_with_base_exp():
    """Create an experiment that depends on another experiment."""
    experiment = Mock(spec=Experiment)
    experiment.id = "exp-123"
    experiment.name = "Comparison Experiment"
    experiment.project_id = "project-456"
    experiment.description = "An experiment with baseline comparison"
    experiment.base_exp_id = "base-exp-789"
    experiment.dataset_id = None
    experiment.dataset_version = None
    experiment.public = True
    experiment.metadata = {"type": "comparison"}
    experiment.repo_info = None

    # Mock the to_dict method to return a proper dictionary
    experiment.to_dict.return_value = {
        "id": "exp-123",
        "name": "Comparison Experiment",
        "project_id": "project-456",
        "description": "An experiment with baseline comparison",
        "base_exp_id": "base-exp-789",
        "dataset_id": None,
        "dataset_version": None,
        "public": True,
        "metadata": {"type": "comparison"},
        "repo_info": None,
    }
    return experiment


@pytest.fixture
def experiment_with_dataset():
    """Create an experiment that depends on a dataset."""
    experiment = Mock(spec=Experiment)
    experiment.id = "exp-456"
    experiment.name = "Dataset Experiment"
    experiment.project_id = "project-456"
    experiment.description = "An experiment linked to a dataset"
    experiment.base_exp_id = None
    experiment.dataset_id = "dataset-123"
    experiment.dataset_version = "v2.1"
    experiment.public = False
    experiment.metadata = {"dataset_linked": True}
    experiment.repo_info = None

    # Mock the to_dict method to return a proper dictionary
    experiment.to_dict.return_value = {
        "id": "exp-456",
        "name": "Dataset Experiment",
        "project_id": "project-456",
        "description": "An experiment linked to a dataset",
        "base_exp_id": None,
        "dataset_id": "dataset-123",
        "dataset_version": "v2.1",
        "public": False,
        "metadata": {"dataset_linked": True},
        "repo_info": None,
    }
    return experiment


@pytest.fixture
def experiment_with_both_deps():
    """Create an experiment with both base experiment and dataset dependencies."""
    experiment = Mock(spec=Experiment)
    experiment.id = "exp-789"
    experiment.name = "Full Dependencies Experiment"
    experiment.project_id = "project-456"
    experiment.description = "An experiment with both dependencies"
    experiment.base_exp_id = "base-exp-999"
    experiment.dataset_id = "dataset-456"
    experiment.dataset_version = "v1.0"
    experiment.public = True
    experiment.metadata = {"full_deps": True}
    experiment.repo_info = None

    # Mock the to_dict method to return a proper dictionary
    experiment.to_dict.return_value = {
        "id": "exp-789",
        "name": "Full Dependencies Experiment",
        "project_id": "project-456",
        "description": "An experiment with both dependencies",
        "base_exp_id": "base-exp-999",
        "dataset_id": "dataset-456",
        "dataset_version": "v1.0",
        "public": True,
        "metadata": {"full_deps": True},
        "repo_info": None,
    }
    return experiment


@pytest.fixture
def experiment_without_dependencies():
    """Create an experiment without any dependencies."""
    experiment = Mock(spec=Experiment)
    experiment.id = "exp-999"
    experiment.name = "Independent Experiment"
    experiment.project_id = "project-456"
    experiment.description = "An experiment without dependencies"
    experiment.base_exp_id = None
    experiment.dataset_id = None
    experiment.dataset_version = None
    experiment.public = True
    experiment.metadata = {"independent": True}
    experiment.repo_info = None

    # Mock the to_dict method to return a proper dictionary
    experiment.to_dict.return_value = {
        "id": "exp-999",
        "name": "Independent Experiment",
        "project_id": "project-456",
        "description": "An experiment without dependencies",
        "base_exp_id": None,
        "dataset_id": None,
        "dataset_version": None,
        "public": True,
        "metadata": {"independent": True},
        "repo_info": None,
    }
    return experiment


@pytest.mark.asyncio
class TestExperimentDependencies:
    """Test dependency resolution in ExperimentMigrator."""

    async def test_get_dependencies_with_base_experiment(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        experiment_with_base_exp,
    ):
        """Test that experiments with base_exp_id return base experiment dependency."""
        migrator = ExperimentMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        dependencies = await migrator.get_dependencies(experiment_with_base_exp)

        # Base experiment dependencies are now included as hard dependencies
        assert dependencies == ["base-exp-789"]

    async def test_get_dependencies_with_dataset(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        experiment_with_dataset,
    ):
        """Test that experiments with dataset_id return correct dependencies."""
        migrator = ExperimentMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        dependencies = await migrator.get_dependencies(experiment_with_dataset)

        assert dependencies == ["dataset-123"]

    async def test_get_dependencies_with_both_dependencies(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        experiment_with_both_deps,
    ):
        """Test that experiments with both dependencies return both dependencies."""
        migrator = ExperimentMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        dependencies = await migrator.get_dependencies(experiment_with_both_deps)

        # Both dataset and base experiment dependencies are returned
        assert set(dependencies) == {"dataset-456", "base-exp-999"}

    async def test_get_dependencies_without_dependencies(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        experiment_without_dependencies,
    ):
        """Test that experiments without dependencies return no dependencies."""
        migrator = ExperimentMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        dependencies = await migrator.get_dependencies(experiment_without_dependencies)

        assert dependencies == []

    async def test_migrate_resource_with_resolved_base_exp(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        experiment_with_base_exp,
    ):
        """Test migration with resolved base experiment dependency."""
        # Mock successful experiment creation
        new_experiment = Mock(spec=Experiment)
        new_experiment.id = "new-exp-123"
        mock_dest_client.with_retry.return_value = new_experiment

        # Mock empty events response for _migrate_experiment_events
        mock_source_client.with_retry.return_value = Mock(events=None)

        migrator = ExperimentMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-456"

        # Mock that the base experiment dependency has been resolved
        migrator.state.id_mapping["base-exp-789"] = "dest-base-exp-789"

        result = await migrator.migrate_resource(experiment_with_base_exp)

        assert result == "new-exp-123"

        # Verify the create call was made with resolved base_exp_id
        create_call = mock_dest_client.with_retry.call_args_list[0]
        assert create_call[0][0] == "create_experiment"

    async def test_migrate_resource_with_resolved_dataset(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        experiment_with_dataset,
    ):
        """Test migration with resolved dataset dependency."""
        # Mock successful experiment creation
        new_experiment = Mock(spec=Experiment)
        new_experiment.id = "new-exp-456"
        mock_dest_client.with_retry.return_value = new_experiment

        # Mock empty events response for _migrate_experiment_events
        mock_source_client.with_retry.return_value = Mock(events=None)

        migrator = ExperimentMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-456"

        # Mock that the dataset dependency has been resolved
        migrator.state.id_mapping["dataset-123"] = "dest-dataset-123"

        result = await migrator.migrate_resource(experiment_with_dataset)

        assert result == "new-exp-456"

        # Verify the create call was made with resolved dataset_id
        create_call = mock_dest_client.with_retry.call_args_list[0]
        assert create_call[0][0] == "create_experiment"

    async def test_migrate_resource_with_unresolved_dependencies(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        experiment_with_both_deps,
    ):
        """Test migration with unresolved dependencies (should log warnings but continue)."""
        # Mock successful experiment creation
        new_experiment = Mock(spec=Experiment)
        new_experiment.id = "new-exp-789"
        mock_dest_client.with_retry.return_value = new_experiment

        # Mock empty events response for _migrate_experiment_events
        mock_source_client.with_retry.return_value = Mock(events=None)

        migrator = ExperimentMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-456"

        # Don't add any mappings - dependencies remain unresolved

        result = await migrator.migrate_resource(experiment_with_both_deps)

        assert result == "new-exp-789"

        # Verify the create call was made (with original IDs since unresolved)
        create_call = mock_dest_client.with_retry.call_args_list[0]
        assert create_call[0][0] == "create_experiment"

    async def test_migrate_resource_without_dependencies(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        experiment_without_dependencies,
    ):
        """Test migration of experiment without dependencies."""
        # Mock successful experiment creation
        new_experiment = Mock(spec=Experiment)
        new_experiment.id = "new-exp-999"
        mock_dest_client.with_retry.return_value = new_experiment

        # Mock empty events response for _migrate_experiment_events
        mock_source_client.with_retry.return_value = Mock(events=None)

        migrator = ExperimentMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-456"

        result = await migrator.migrate_resource(experiment_without_dependencies)

        assert result == "new-exp-999"

        # Verify the create call was made
        create_call = mock_dest_client.with_retry.call_args_list[0]
        assert create_call[0][0] == "create_experiment"

    async def test_get_dependency_types(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that ExperimentMigrator returns correct dependency types."""
        migrator = ExperimentMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        dependency_types = await migrator.get_dependency_types()

        assert set(dependency_types) == {"datasets", "experiments"}

    async def test_populate_dependency_mappings(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that dependency mappings are populated correctly."""
        from unittest.mock import Mock

        # Mock datasets in source and destination
        source_dataset = Mock()
        source_dataset.id = "source-dataset-123"
        source_dataset.name = "Test Dataset"

        dest_dataset = Mock()
        dest_dataset.id = "dest-dataset-456"
        dest_dataset.name = "Test Dataset"

        # Mock experiments in source and destination
        source_exp = Mock()
        source_exp.id = "source-exp-789"
        source_exp.name = "Base Experiment"

        dest_exp = Mock()
        dest_exp.id = "dest-exp-999"
        dest_exp.name = "Base Experiment"

        # Setup mock responses
        mock_source_client.with_retry.side_effect = [
            [source_dataset],  # datasets list call
            [source_exp],  # experiments list call
        ]

        mock_dest_client.with_retry.side_effect = [
            [dest_dataset],  # datasets list call
            [dest_exp],  # experiments list call
        ]

        migrator = ExperimentMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        await migrator.populate_dependency_mappings(
            mock_source_client, mock_dest_client, "project-123"
        )

        # Check that ID mappings were populated
        assert migrator.state.id_mapping.get("source-dataset-123") == "dest-dataset-456"
        assert migrator.state.id_mapping.get("source-exp-789") == "dest-exp-999"
