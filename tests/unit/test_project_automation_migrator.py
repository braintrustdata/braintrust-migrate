"""Unit tests for ProjectAutomationMigrator."""

from unittest.mock import AsyncMock

import pytest

from braintrust_migrate.resources.project_automations import (
    ProjectAutomationMigrator,
)


@pytest.fixture
def sample_logs_automation():
    """Create a sample logs-alert automation dict."""
    return {
        "id": "auto-123",
        "name": "High-error alert",
        "project_id": "project-456",
        "description": "Fires when error rate spikes",
        "created": "2024-06-01T00:00:00Z",
        "config": {
            "event_type": "logs",
            "btql_filter": "scores.accuracy < 0.5",
            "interval_seconds": 3600,
            "action": {
                "type": "webhook",
                "url": "https://hooks.example.com/alert",
            },
        },
    }


@pytest.fixture
def sample_export_automation():
    """Create a sample BTQL-export automation dict."""
    return {
        "id": "auto-export-789",
        "name": "Nightly log export",
        "project_id": "project-456",
        "description": "Exports logs to S3 every day",
        "created": "2024-07-01T00:00:00Z",
        "config": {
            "event_type": "btql_export",
            "export_definition": {
                "type": "log_traces",
            },
            "export_path": "s3://my-bucket/exports/logs",
            "format": "parquet",
            "interval_seconds": 86400,
            "credentials": {
                "type": "aws_iam",
                "role_arn": "arn:aws:iam::123456789:role/export-role",
            },
        },
    }


@pytest.fixture
def sample_slack_automation():
    """Create a sample Slack-action automation dict."""
    return {
        "id": "auto-slack-321",
        "name": "Slack quality alert",
        "project_id": "project-456",
        "config": {
            "event_type": "logs",
            "btql_filter": "scores.quality < 0.3",
            "interval_seconds": 1800,
            "action": {
                "type": "slack",
                "workspace_id": "T01ABC",
                "channel": "C01DEF",
                "message_template": "Quality dropped below threshold!",
            },
        },
    }


@pytest.fixture
def minimal_automation():
    """Create a minimal automation dict."""
    return {
        "id": "auto-min-456",
        "name": "Minimal Automation",
        "project_id": "project-456",
    }


@pytest.mark.asyncio
class TestProjectAutomationMigrator:
    """Test ProjectAutomationMigrator functionality."""

    async def test_resource_name(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that resource_name returns correct value."""
        migrator = ProjectAutomationMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        assert migrator.resource_name == "ProjectAutomations"

    async def test_get_dependencies_empty(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_logs_automation,
    ):
        """Test that project automations have no dependencies."""
        migrator = ProjectAutomationMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        dependencies = await migrator.get_dependencies(sample_logs_automation)

        assert dependencies == []

    async def test_list_source_resources_success(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_logs_automation,
    ):
        """Test successful listing of source project automations."""
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_logs_automation]}
        )

        migrator = ProjectAutomationMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        automations = await migrator.list_source_resources()

        assert len(automations) == 1
        assert automations[0] == sample_logs_automation

    async def test_list_source_resources_with_project_filter(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_logs_automation,
    ):
        """Test listing automations with project filter (client-side)."""
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_logs_automation]}
        )

        migrator = ProjectAutomationMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        automations = await migrator.list_source_resources(project_id="project-456")

        assert len(automations) == 1

    async def test_list_source_resources_empty(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test listing automations with empty response."""
        mock_source_client.with_retry = AsyncMock(return_value={"objects": []})

        migrator = ProjectAutomationMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        automations = await migrator.list_source_resources()

        assert len(automations) == 0

    async def test_migrate_resource_success_logs_webhook(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_logs_automation,
    ):
        """Test successful migration of a logs/webhook automation."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-auto-789", "name": "High-error alert"}
        )

        migrator = ProjectAutomationMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        result = await migrator.migrate_resource(sample_logs_automation)

        assert result == "new-auto-789"

    async def test_migrate_resource_success_export(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_export_automation,
    ):
        """Test successful migration of a BTQL-export automation."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-export-789", "name": "Nightly log export"}
        )

        migrator = ProjectAutomationMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        result = await migrator.migrate_resource(sample_export_automation)

        assert result == "new-export-789"

    async def test_migrate_resource_success_slack_action(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_slack_automation,
    ):
        """Test successful migration of a Slack-action automation."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-slack-789", "name": "Slack quality alert"}
        )

        migrator = ProjectAutomationMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        result = await migrator.migrate_resource(sample_slack_automation)

        assert result == "new-slack-789"

    async def test_migrate_resource_success_minimal(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        minimal_automation,
    ):
        """Test migration of an automation with minimal fields."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-auto-min", "name": "Minimal Automation"}
        )

        migrator = ProjectAutomationMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        result = await migrator.migrate_resource(minimal_automation)

        assert result == "new-auto-min"

    async def test_migrate_resource_uses_id_mapping_for_project(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_logs_automation,
    ):
        """Test that migrate_resource resolves project_id via id_mapping."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-auto-mapped", "name": "High-error alert"}
        )

        migrator = ProjectAutomationMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        # No dest_project_id set; rely on id_mapping instead
        migrator.dest_project_id = None
        migrator.state.id_mapping["project-456"] = "mapped-dest-project"

        result = await migrator.migrate_resource(sample_logs_automation)

        assert result == "new-auto-mapped"

    async def test_migrate_resource_no_project_mapping(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_logs_automation,
    ):
        """Test migration fails without project mapping."""
        migrator = ProjectAutomationMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = None

        with pytest.raises(ValueError, match="No destination project mapping"):
            await migrator.migrate_resource(sample_logs_automation)

    async def test_migrate_resource_creation_error(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_logs_automation,
    ):
        """Test handling of creation errors during migration."""
        mock_dest_client.with_retry = AsyncMock(
            side_effect=Exception("API error creating automation")
        )

        migrator = ProjectAutomationMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        with pytest.raises(Exception, match="Failed to migrate project automation"):
            await migrator.migrate_resource(sample_logs_automation)

    async def test_migrate_resource_no_id_returned(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_logs_automation,
    ):
        """Test that missing ID in API response raises an error."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"name": "High-error alert"}  # no "id" field
        )

        migrator = ProjectAutomationMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        with pytest.raises(Exception, match="No ID returned"):
            await migrator.migrate_resource(sample_logs_automation)
