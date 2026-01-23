"""Unit tests for OpenAPI-based field inclusion in migrators."""

import pytest

from braintrust_migrate.resources.base import ResourceMigrator
from braintrust_migrate.resources.datasets import DatasetMigrator
from braintrust_migrate.resources.experiments import ExperimentMigrator
from braintrust_migrate.resources.functions import FunctionMigrator
from braintrust_migrate.resources.logs import LogsMigrator
from braintrust_migrate.resources.prompts import PromptMigrator


@pytest.mark.asyncio
class TestOpenAPIFieldInclusion:
    """Test that migrators use OpenAPI-based field inclusion."""

    async def test_base_migrator_allowed_fields_fallback(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that base migrator returns None for unknown resource types."""

        # Create a simple concrete implementation for testing
        class TestMigrator(ResourceMigrator):
            @property
            def resource_name(self) -> str:
                return "UnknownResources"  # This won't exist in OpenAPI spec

            async def list_source_resources(self, project_id=None):
                return []

            async def migrate_resource(self, resource):
                return "test-id"

        migrator = TestMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Should return None for unknown resource type (no schema found)
        assert migrator.allowed_fields_for_insert is None

    async def test_prompt_migrator_uses_openapi_fields(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that PromptMigrator uses OpenAPI-determined allowed fields."""
        migrator = PromptMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Should have allowed fields from OpenAPI spec
        allowed_fields = migrator.allowed_fields_for_insert
        expected_allowed = {
            "description",
            "prompt_data",
            "slug",
            "tags",
            "function_type",
            "project_id",
            "name",
        }
        assert allowed_fields == expected_allowed

    async def test_dataset_migrator_uses_openapi_fields(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that DatasetMigrator uses OpenAPI-determined allowed fields."""
        migrator = DatasetMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Should have allowed fields from OpenAPI spec
        allowed_fields = migrator.allowed_fields_for_insert
        expected_allowed = {
            "metadata",
            "name",
            "description",
            "project_id",
        }
        assert allowed_fields == expected_allowed

    async def test_experiment_migrator_uses_openapi_fields(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that ExperimentMigrator uses OpenAPI-determined allowed fields."""
        migrator = ExperimentMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Should have allowed fields from OpenAPI spec
        allowed_fields = migrator.allowed_fields_for_insert
        expected_allowed = {
            "ensure_new",
            "public",
            "base_exp_id",
            "repo_info",
            "metadata",
            "dataset_version",
            "description",
            "dataset_id",
            "project_id",
            "name",
            "tags",
        }
        assert allowed_fields == expected_allowed

    async def test_function_migrator_uses_openapi_fields(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that FunctionMigrator uses OpenAPI-determined allowed fields."""
        migrator = FunctionMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Should have allowed fields from OpenAPI spec
        allowed_fields = migrator.allowed_fields_for_insert
        expected_allowed = {
            "name",
            "project_id",
            "slug",
            "function_data",
            "origin",
            "prompt_data",
            "tags",
            "function_schema",
            "description",
            "function_type",
        }
        assert allowed_fields == expected_allowed

    async def test_logs_migrator_has_explicit_insert_allowlist(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that LogsMigrator has a stable allow-list for insert payloads."""
        migrator = LogsMigrator(
            mock_source_client,
            mock_dest_client,
            temp_checkpoint_dir,
            page_limit=1,
            insert_batch_size=1,
        )

        insert_fields = migrator._INSERT_FIELDS
        assert isinstance(insert_fields, set)
        assert "_is_merge" in insert_fields
        assert "_merge_paths" in insert_fields
        # Docs include this even though older specs may omit it
        assert "_array_delete" in insert_fields

    async def test_all_migrators_use_consistent_approach(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that all migrators use the same OpenAPI-based approach."""
        dataset_migrator = DatasetMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        experiment_migrator = ExperimentMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        function_migrator = FunctionMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        # LogsMigrator is streaming-only and does not use OpenAPI create schemas.
        # It maintains a manual allow-list for InsertProjectLogsEvent.
        logs_migrator = LogsMigrator(
            mock_source_client,
            mock_dest_client,
            temp_checkpoint_dir,
            page_limit=1,
            insert_batch_size=1,
        )
        prompt_migrator = PromptMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # All migrators should either have allowed fields (if schema exists) or None
        for migrator in [
            dataset_migrator,
            experiment_migrator,
            function_migrator,
            prompt_migrator,
        ]:
            allowed_fields = migrator.allowed_fields_for_insert
            assert allowed_fields is None or isinstance(allowed_fields, set)

            # If they have allowed fields, they should be non-empty
            if allowed_fields is not None:
                assert len(allowed_fields) > 0

        # Known schemas should have allowed fields
        assert prompt_migrator.allowed_fields_for_insert is not None
        assert dataset_migrator.allowed_fields_for_insert is not None
        assert experiment_migrator.allowed_fields_for_insert is not None
        assert function_migrator.allowed_fields_for_insert is not None

        # LogsMigrator is validated separately via explicit allow-list
        assert isinstance(logs_migrator._INSERT_FIELDS, set)

    async def test_resource_name_to_schema_mapping(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that resource names are correctly mapped to schema names."""
        # Test the resource name conversion logic
        prompt_migrator = PromptMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # "Prompts" -> "Prompt" -> "CreatePrompt" schema
        assert prompt_migrator.resource_name == "Prompts"
        assert prompt_migrator.allowed_fields_for_insert is not None

        dataset_migrator = DatasetMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # "Datasets" -> "Dataset" -> "CreateDataset" schema
        assert dataset_migrator.resource_name == "Datasets"
        assert dataset_migrator.allowed_fields_for_insert is not None
