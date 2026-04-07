"""Unit tests for configuration module."""

from pathlib import Path

import pytest

from braintrust_migrate.config import BraintrustOrgConfig, Config, MigrationConfig

# Test constants
DEFAULT_BATCH_SIZE = 100
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_MAX_CONCURRENT = 1
DEFAULT_CHECKPOINT_INTERVAL = 50
TEST_BATCH_SIZE = 50
TEST_RETRY_ATTEMPTS = 5


class TestBraintrustOrgConfig:
    """Test Braintrust organization configuration."""

    def test_valid_config(self):
        """Test creating a valid org config."""
        config = BraintrustOrgConfig(
            api_key="test-key-123", url="https://www.braintrust.dev"
        )
        assert config.api_key == "test-key-123"
        assert str(config.url) == "https://www.braintrust.dev/"

    def test_empty_api_key_validation(self):
        """Test that empty API key raises validation error."""
        with pytest.raises(ValueError, match="API key cannot be empty"):
            BraintrustOrgConfig(api_key="", url="https://www.braintrust.dev")

    def test_whitespace_api_key_validation(self):
        """Test that whitespace-only API key raises validation error."""
        with pytest.raises(ValueError, match="API key cannot be empty"):
            BraintrustOrgConfig(api_key="   ", url="https://www.braintrust.dev")


class TestMigrationConfig:
    """Test migration configuration."""

    def test_default_values(self):
        """Test default configuration values."""
        config = MigrationConfig()
        assert config.batch_size == DEFAULT_BATCH_SIZE
        assert config.retry_attempts == DEFAULT_RETRY_ATTEMPTS
        assert config.retry_delay == 1.0
        assert config.max_concurrent == DEFAULT_MAX_CONCURRENT
        assert config.checkpoint_interval == DEFAULT_CHECKPOINT_INTERVAL

    def test_validation_bounds(self):
        """Test configuration validation bounds."""
        # Test valid bounds
        config = MigrationConfig(
            batch_size=TEST_BATCH_SIZE,
            retry_attempts=TEST_RETRY_ATTEMPTS,
            retry_delay=0.5,
            max_concurrent=20,
            checkpoint_interval=25,
        )
        assert config.batch_size == TEST_BATCH_SIZE
        assert config.retry_attempts == TEST_RETRY_ATTEMPTS

        # Test invalid bounds
        with pytest.raises(ValueError, match="batch_size"):
            MigrationConfig(batch_size=0)  # Below minimum

        with pytest.raises(ValueError, match="batch_size"):
            MigrationConfig(batch_size=2000)  # Above maximum

    def test_acl_auto_invite_requires_acl_map_users(self):
        """Test ACL auto-invite cannot be enabled without ACL user mapping."""
        with pytest.raises(
            ValueError,
            match="acl_auto_invite_users requires acl_map_users",
        ):
            MigrationConfig(acl_auto_invite_users=True, acl_map_users=False)

    def test_group_auto_invite_requires_group_map_users(self):
        """Test group auto-invite cannot be enabled without group user mapping."""
        with pytest.raises(
            ValueError,
            match="group_auto_invite_users requires group_map_users",
        ):
            MigrationConfig(group_auto_invite_users=True, group_map_users=False)


class TestConfig:
    """Test main configuration class."""

    def test_checkpoint_dir_methods(self):
        """Test checkpoint directory methods."""
        config = Config(
            source=BraintrustOrgConfig(api_key="source-key"),
            destination=BraintrustOrgConfig(api_key="dest-key"),
            state_dir=Path("/tmp/test-checkpoints"),
        )

        # Test general checkpoint dir
        general_dir = config.get_checkpoint_dir()
        assert general_dir == Path("/tmp/test-checkpoints")

        # Test project-specific checkpoint dir
        project_dir = config.get_checkpoint_dir("my-project")
        assert project_dir == Path("/tmp/test-checkpoints/my-project")


class TestConfigFromEnv:
    """Test configuration loading from environment variables."""

    def test_missing_required_env_vars(self, monkeypatch):
        """Test that missing required env vars raise ValueError."""
        # Clear all BT env vars
        for key in ["BT_SOURCE_API_KEY", "BT_DEST_API_KEY"]:
            monkeypatch.delenv(key, raising=False)

        with pytest.raises(ValueError, match="BT_SOURCE_API_KEY"):
            Config.from_env()

    def test_valid_env_config(self, monkeypatch):
        """Test loading valid config from environment."""
        # Set required env vars
        monkeypatch.setenv("BT_SOURCE_API_KEY", "source-test-key")
        monkeypatch.setenv("BT_DEST_API_KEY", "dest-test-key")

        # Set optional env vars
        monkeypatch.setenv("BT_SOURCE_URL", "https://source.example.com")
        monkeypatch.setenv("BT_DEST_URL", "https://dest.example.com")
        monkeypatch.setenv("MIGRATION_BATCH_SIZE", "50")
        monkeypatch.setenv("MIGRATION_ACL_MAP_USERS", "true")
        monkeypatch.setenv("MIGRATION_ACL_AUTO_INVITE_USERS", "true")
        monkeypatch.setenv("MIGRATION_GROUP_MAP_USERS", "true")
        monkeypatch.setenv("MIGRATION_GROUP_AUTO_INVITE_USERS", "true")
        monkeypatch.setenv("MIGRATION_EVENTS_FETCH_GROUP_SIZE", "17")
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")

        config = Config.from_env()

        assert config.source.api_key == "source-test-key"
        assert config.destination.api_key == "dest-test-key"
        assert str(config.source.url) == "https://source.example.com/"
        assert str(config.destination.url) == "https://dest.example.com/"
        assert config.migration.batch_size == TEST_BATCH_SIZE
        assert config.migration.acl_map_users is True
        assert config.migration.acl_auto_invite_users is True
        assert config.migration.group_map_users is True
        assert config.migration.group_auto_invite_users is True
        assert config.migration.events_fetch_group_size == 17
        assert config.logging.level == "DEBUG"
