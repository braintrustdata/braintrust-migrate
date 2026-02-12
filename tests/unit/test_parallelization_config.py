"""Unit tests for parallelization configuration fields."""

import pytest

from braintrust_migrate.config import Config, BraintrustOrgConfig, MigrationConfig


class TestParallelizationConfigDefaults:
    """Verify defaults for new parallelization config fields."""

    def test_max_concurrent_resources_default(self):
        config = MigrationConfig()
        assert config.max_concurrent_resources == 5

    def test_streaming_pipeline_default(self):
        config = MigrationConfig()
        assert config.streaming_pipeline is True

    def test_max_concurrent_requests_default(self):
        config = MigrationConfig()
        assert config.max_concurrent_requests == 20


class TestParallelizationConfigBounds:
    """Verify validation bounds for new fields."""

    def test_max_concurrent_resources_valid_range(self):
        config = MigrationConfig(max_concurrent_resources=1)
        assert config.max_concurrent_resources == 1

        config = MigrationConfig(max_concurrent_resources=50)
        assert config.max_concurrent_resources == 50

    def test_max_concurrent_resources_below_min(self):
        with pytest.raises(ValueError):
            MigrationConfig(max_concurrent_resources=0)

    def test_max_concurrent_resources_above_max(self):
        with pytest.raises(ValueError):
            MigrationConfig(max_concurrent_resources=51)

    def test_max_concurrent_requests_valid_range(self):
        config = MigrationConfig(max_concurrent_requests=1)
        assert config.max_concurrent_requests == 1

        config = MigrationConfig(max_concurrent_requests=200)
        assert config.max_concurrent_requests == 200

    def test_max_concurrent_requests_below_min(self):
        with pytest.raises(ValueError):
            MigrationConfig(max_concurrent_requests=0)

    def test_max_concurrent_requests_above_max(self):
        with pytest.raises(ValueError):
            MigrationConfig(max_concurrent_requests=201)

    def test_streaming_pipeline_bool(self):
        config = MigrationConfig(streaming_pipeline=False)
        assert config.streaming_pipeline is False

        config = MigrationConfig(streaming_pipeline=True)
        assert config.streaming_pipeline is True


class TestParallelizationConfigFromEnv:
    """Verify env var parsing for parallelization fields."""

    def test_max_concurrent_resources_from_env(self, monkeypatch):
        monkeypatch.setenv("BT_SOURCE_API_KEY", "src")
        monkeypatch.setenv("BT_DEST_API_KEY", "dst")
        monkeypatch.setenv("MIGRATION_MAX_CONCURRENT_RESOURCES", "10")

        config = Config.from_env()
        assert config.migration.max_concurrent_resources == 10

    def test_streaming_pipeline_from_env_true(self, monkeypatch):
        monkeypatch.setenv("BT_SOURCE_API_KEY", "src")
        monkeypatch.setenv("BT_DEST_API_KEY", "dst")
        monkeypatch.setenv("MIGRATION_STREAMING_PIPELINE", "true")

        config = Config.from_env()
        assert config.migration.streaming_pipeline is True

    def test_streaming_pipeline_from_env_false(self, monkeypatch):
        monkeypatch.setenv("BT_SOURCE_API_KEY", "src")
        monkeypatch.setenv("BT_DEST_API_KEY", "dst")
        monkeypatch.setenv("MIGRATION_STREAMING_PIPELINE", "false")

        config = Config.from_env()
        assert config.migration.streaming_pipeline is False

    def test_max_concurrent_requests_from_env(self, monkeypatch):
        monkeypatch.setenv("BT_SOURCE_API_KEY", "src")
        monkeypatch.setenv("BT_DEST_API_KEY", "dst")
        monkeypatch.setenv("MIGRATION_MAX_CONCURRENT_REQUESTS", "50")

        config = Config.from_env()
        assert config.migration.max_concurrent_requests == 50

    def test_defaults_when_env_not_set(self, monkeypatch):
        monkeypatch.setenv("BT_SOURCE_API_KEY", "src")
        monkeypatch.setenv("BT_DEST_API_KEY", "dst")
        # Ensure the parallelization env vars are NOT set
        monkeypatch.delenv("MIGRATION_MAX_CONCURRENT_RESOURCES", raising=False)
        monkeypatch.delenv("MIGRATION_STREAMING_PIPELINE", raising=False)
        monkeypatch.delenv("MIGRATION_MAX_CONCURRENT_REQUESTS", raising=False)

        config = Config.from_env()
        assert config.migration.max_concurrent_resources == 5
        assert config.migration.streaming_pipeline is True
        assert config.migration.max_concurrent_requests == 20
