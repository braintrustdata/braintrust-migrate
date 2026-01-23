"""Unit tests for ProjectScoreMigrator."""

from unittest.mock import AsyncMock

import pytest

from braintrust_migrate.resources.project_scores import ProjectScoreMigrator


@pytest.fixture
def sample_project_score():
    """Create a sample project score dict."""
    return {
        "id": "score-123",
        "name": "Test Score",
        "project_id": "project-456",
        "score_type": "slider",
        "description": "A test score",
        "created": "2024-01-01T00:00:00Z",
    }


@pytest.fixture
def project_score_with_config():
    """Create a project score with online scoring config."""
    return {
        "id": "score-config-123",
        "name": "Score with Config",
        "project_id": "project-456",
        "score_type": "slider",
        "description": "A score with online config",
        "config": {
            "online": {
                "sampling_rate": 0.5,
                "scorers": [
                    {"type": "function", "id": "func-123"},
                    {"type": "function", "id": "func-456"},
                ],
            }
        },
    }


@pytest.fixture
def minimal_project_score():
    """Create a minimal project score dict."""
    return {
        "id": "score-min-456",
        "name": "Minimal Score",
        "project_id": "project-456",
        "score_type": "slider",
    }


@pytest.mark.asyncio
class TestProjectScoreMigrator:
    """Test ProjectScoreMigrator functionality."""

    async def test_resource_name(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that resource_name returns correct value."""
        migrator = ProjectScoreMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        assert migrator.resource_name == "ProjectScores"

    async def test_list_source_resources_no_filter(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_project_score,
    ):
        """Test listing all project scores."""
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_project_score]}
        )

        migrator = ProjectScoreMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        scores = await migrator.list_source_resources()

        assert len(scores) == 1
        assert scores[0] == sample_project_score

    async def test_list_source_resources_with_project_filter(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_project_score,
    ):
        """Test listing project scores with project filter."""
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_project_score]}
        )

        migrator = ProjectScoreMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        scores = await migrator.list_source_resources(project_id="project-456")

        assert len(scores) == 1

    async def test_list_source_resources_async_iterator(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_project_score,
    ):
        """Test listing project scores with raw API response."""
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_project_score]}
        )

        migrator = ProjectScoreMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        scores = await migrator.list_source_resources()

        assert len(scores) == 1

    async def test_list_source_resources_direct_list(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test listing project scores with empty response."""
        mock_source_client.with_retry = AsyncMock(return_value={"objects": []})

        migrator = ProjectScoreMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        scores = await migrator.list_source_resources()

        assert len(scores) == 0

    async def test_migrate_resource_success(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_project_score,
    ):
        """Test successful migration of a project score."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-score-789", "name": "Test Score"}
        )

        migrator = ProjectScoreMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        result = await migrator.migrate_resource(sample_project_score)

        assert result == "new-score-789"

    async def test_migrate_resource_with_config(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        project_score_with_config,
    ):
        """Test migration of project score with config."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-score-config", "name": "Score with Config"}
        )

        migrator = ProjectScoreMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"
        # Add function mappings
        migrator.state.id_mapping["func-123"] = "dest-func-123"
        migrator.state.id_mapping["func-456"] = "dest-func-456"

        result = await migrator.migrate_resource(project_score_with_config)

        assert result == "new-score-config"

    async def test_migrate_resource_no_project_mapping(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_project_score,
    ):
        """Test migration uses dest_project_id as fallback."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-score-789", "name": "Test Score"}
        )

        migrator = ProjectScoreMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-fallback"

        result = await migrator.migrate_resource(sample_project_score)

        assert result == "new-score-789"

    async def test_migrate_resource_no_project_mapping_or_dest_id(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_project_score,
    ):
        """Test migration fails without any project mapping."""
        migrator = ProjectScoreMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = None

        with pytest.raises(ValueError, match="No destination project mapping"):
            await migrator.migrate_resource(sample_project_score)

    async def test_migrate_resource_error(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_project_score,
    ):
        """Test handling of creation errors."""
        mock_dest_client.with_retry = AsyncMock(
            side_effect=Exception("API error creating score")
        )

        migrator = ProjectScoreMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        with pytest.raises(Exception, match="Failed to migrate project score"):
            await migrator.migrate_resource(sample_project_score)

    async def test_resolve_config_dependencies_no_online(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test config resolution with no online config."""
        migrator = ProjectScoreMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        config = {"some_key": "value"}
        result = await migrator._resolve_config_dependencies(config)

        assert result == config

    async def test_resolve_config_dependencies_with_scorers(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test config resolution with scorers."""
        migrator = ProjectScoreMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.state.id_mapping["func-123"] = "dest-func-123"

        config = {
            "online": {
                "scorers": [{"type": "function", "id": "func-123"}],
            }
        }
        result = await migrator._resolve_config_dependencies(config)

        # The function reference should be resolved
        assert result["online"]["scorers"][0]["id"] == "dest-func-123"

    async def test_get_dependencies_basic(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_project_score,
    ):
        """Test getting basic dependencies (project)."""
        migrator = ProjectScoreMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        deps = await migrator.get_dependencies(sample_project_score)

        assert "project-456" in deps

    async def test_get_dependencies_with_function_scorers(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        project_score_with_config,
    ):
        """Test getting dependencies including function scorers."""
        migrator = ProjectScoreMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        deps = await migrator.get_dependencies(project_score_with_config)

        assert "project-456" in deps
        assert "func-123" in deps
        assert "func-456" in deps

    async def test_get_dependencies_no_config(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        minimal_project_score,
    ):
        """Test getting dependencies with no config."""
        migrator = ProjectScoreMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        deps = await migrator.get_dependencies(minimal_project_score)

        assert deps == ["project-456"]
