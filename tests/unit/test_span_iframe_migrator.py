"""Unit tests for SpanIframeMigrator."""

from unittest.mock import AsyncMock

import pytest

from braintrust_migrate.resources.span_iframes import SpanIframeMigrator


@pytest.fixture
def sample_span_iframe():
    """Create a sample span iframe dict."""
    return {
        "id": "iframe-123",
        "name": "Test Iframe",
        "project_id": "project-456",
        "url": "https://example.com/viewer",
        "description": "A test span iframe",
        "post_message": True,
        "created": "2024-01-01T00:00:00Z",
    }


@pytest.fixture
def minimal_span_iframe():
    """Create a minimal span iframe dict."""
    return {
        "id": "iframe-min-456",
        "name": "Minimal Iframe",
        "project_id": "project-456",
        "url": "https://example.com/minimal",
    }


@pytest.mark.asyncio
class TestSpanIframeMigrator:
    """Test SpanIframeMigrator functionality."""

    async def test_resource_name(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that resource_name returns correct value."""
        migrator = SpanIframeMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        assert migrator.resource_name == "SpanIFrames"

    async def test_get_dependencies_empty(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_span_iframe,
    ):
        """Test that span iframes have no dependencies."""
        migrator = SpanIframeMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        deps = await migrator.get_dependencies(sample_span_iframe)

        assert deps == []

    async def test_list_source_resources_all(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_span_iframe,
    ):
        """Test listing all span iframes."""
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_span_iframe]}
        )

        migrator = SpanIframeMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        iframes = await migrator.list_source_resources()

        assert len(iframes) == 1
        assert iframes[0] == sample_span_iframe

    async def test_list_source_resources_filtered_by_project(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_span_iframe,
    ):
        """Test listing span iframes filtered by project."""
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_span_iframe]}
        )

        migrator = SpanIframeMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        iframes = await migrator.list_source_resources(project_id="project-456")

        assert len(iframes) == 1

    async def test_list_source_resources_async_iterator(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_span_iframe,
    ):
        """Test listing span iframes with raw API response."""
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_span_iframe]}
        )

        migrator = SpanIframeMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        iframes = await migrator.list_source_resources()

        assert len(iframes) == 1

    async def test_migrate_resource_full(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_span_iframe,
    ):
        """Test successful migration of a span iframe with all fields."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-iframe-789", "name": "Test Iframe"}
        )

        migrator = SpanIframeMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        result = await migrator.migrate_resource(sample_span_iframe)

        assert result == "new-iframe-789"

    async def test_migrate_resource_minimal(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        minimal_span_iframe,
    ):
        """Test migration of span iframe with minimal fields."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-iframe-min", "name": "Minimal Iframe"}
        )

        migrator = SpanIframeMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        result = await migrator.migrate_resource(minimal_span_iframe)

        assert result == "new-iframe-min"

    async def test_migrate_resource_error(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_span_iframe,
    ):
        """Test handling of creation errors."""
        mock_dest_client.with_retry = AsyncMock(
            side_effect=Exception("API error creating iframe")
        )

        migrator = SpanIframeMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        with pytest.raises(Exception, match="API error creating iframe"):
            await migrator.migrate_resource(sample_span_iframe)

    async def test_migrate_resource_preserves_optional_fields(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_span_iframe,
    ):
        """Test that optional fields are preserved during migration."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={
                "id": "new-iframe-789",
                "name": "Test Iframe",
                "description": "A test span iframe",
                "post_message": True,
            }
        )

        migrator = SpanIframeMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        result = await migrator.migrate_resource(sample_span_iframe)

        assert result == "new-iframe-789"

    async def test_migrate_resource_skips_none_optional_fields(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that None optional fields are skipped."""
        iframe_with_none = {
            "id": "iframe-none-123",
            "name": "Iframe with None",
            "project_id": "project-456",
            "url": "https://example.com",
            "description": None,
            "post_message": None,
        }

        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-iframe-none", "name": "Iframe with None"}
        )

        migrator = SpanIframeMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        result = await migrator.migrate_resource(iframe_with_none)

        assert result == "new-iframe-none"
