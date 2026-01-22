"""Configuration models and environment variable parsing for Braintrust migration tool."""

import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from dotenv import load_dotenv
from pydantic import BaseModel, Field, HttpUrl, field_validator

# Load environment variables from .env file if it exists
load_dotenv()

_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def canonicalize_created_after(value: str) -> str:
    """Normalize a user-supplied datetime string for BTQL `created >= ...` filters.

    Accepts:
    - YYYY-MM-DD (treated as midnight UTC)
    - ISO-8601 datetimes, with or without timezone (naive treated as UTC)

    Returns a canonical UTC ISO-8601 string ending in 'Z'.
    """
    v = (value or "").strip()
    if not v:
        raise ValueError("created_after cannot be empty")

    if _DATE_ONLY_RE.match(v):
        dt = datetime.fromisoformat(v).replace(tzinfo=UTC)
        return dt.isoformat().replace("+00:00", "Z")

    v_for_parse = v[:-1] + "+00:00" if v.endswith("Z") else v
    try:
        dt = datetime.fromisoformat(v_for_parse)
    except Exception as e:
        raise ValueError(
            f"Invalid created_after datetime (expected ISO-8601 like 2026-01-15T00:00:00Z): {value!r}"
        ) from e

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    dt = dt.astimezone(UTC)
    return dt.isoformat().replace("+00:00", "Z")


class BraintrustOrgConfig(BaseModel):
    """Configuration for a Braintrust organization."""

    api_key: str = Field(..., description="Braintrust API key")
    url: HttpUrl = Field(
        default_factory=lambda: HttpUrl("https://api.braintrust.dev"),
        description="Braintrust API base URL",
    )

    @field_validator("api_key")
    def validate_api_key(cls, v: str) -> str:
        """Validate that API key is not empty."""
        if not v or v.strip() == "":
            raise ValueError("API key cannot be empty")
        return v.strip()


class MigrationConfig(BaseModel):
    """Configuration for migration behavior."""

    batch_size: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Number of items to process in each batch",
    )
    retry_attempts: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Number of retry attempts for failed operations",
    )
    retry_delay: float = Field(
        default=1.0,
        ge=0.1,
        le=30.0,
        description="Initial delay between retries in seconds",
    )
    max_concurrent: int = Field(
        default=10, ge=1, le=50, description="Maximum number of concurrent operations"
    )
    checkpoint_interval: int = Field(
        default=50, ge=1, description="Write checkpoint every N successful operations"
    )

    # Insert payload sizing (used for byte-aware batching before inserts)
    insert_max_request_bytes: int = Field(
        default=6 * 1024 * 1024,
        ge=1,
        le=500 * 1024 * 1024,
        description=(
            "Maximum allowed HTTP request payload size in bytes for insert calls. "
            "Used for byte-aware batching with headroom. Default is 6MB (common gateway limit)."
        ),
    )
    insert_request_headroom_ratio: float = Field(
        default=0.5,
        ge=0.05,
        le=1.0,
        description=(
            "Headroom ratio applied to insert_max_request_bytes when computing the effective "
            "byte cap for batching (e.g. 0.5 means target <= 50% of the max)."
        ),
    )

    # Logs migration tuning (logs are always streamed/paginated)
    logs_fetch_limit: int = Field(
        default=1000,
        ge=1,
        le=1_000,
        description="Fetch page size for streaming logs migration via BTQL (limit is in rows/spans)",
    )
    logs_insert_batch_size: int = Field(
        default=200,
        ge=1,
        le=1_000,
        description="Insert batch size for streaming logs migration (number of events per insert call)",
    )
    logs_use_version_snapshot: bool = Field(
        default=True,
        description="Pin a stable snapshot version for streaming logs migration",
    )
    logs_use_seen_db: bool = Field(
        default=True,
        description="Use a SQLite seen-id store to prevent older versions overwriting newer ones during pagination",
    )

    created_after: str | None = Field(
        default=None,
        description=(
            "Optional ISO-8601 timestamp filter (logs only). When set, project logs "
            "migration will only migrate events with created >= created_after."
        ),
    )

    # Experiment event migration tuning (experiments can be logs-scale)
    experiment_events_fetch_limit: int = Field(
        default=1000,
        ge=1,
        le=10_000,
        description="Fetch page size for streaming experiment events via BTQL (limit is in rows/spans)",
    )
    experiment_events_insert_batch_size: int = Field(
        default=200,
        ge=1,
        le=10_000,
        description="Insert batch size for streaming experiment events (number of events per insert call)",
    )
    experiment_events_use_version_snapshot: bool = Field(
        default=True,
        description="Pin a stable snapshot version for streaming experiment event migration",
    )
    experiment_events_use_seen_db: bool = Field(
        default=True,
        description="Use a SQLite seen-id store to prevent older versions overwriting newer ones during experiment pagination",
    )

    # Dataset event migration tuning
    dataset_events_fetch_limit: int = Field(
        default=1000,
        ge=1,
        le=10_000,
        description="Fetch page size for streaming dataset events via BTQL (limit is in rows/spans)",
    )
    dataset_events_insert_batch_size: int = Field(
        default=200,
        ge=1,
        le=10_000,
        description="Insert batch size for streaming dataset events (number of events per insert call)",
    )
    dataset_events_use_version_snapshot: bool = Field(
        default=True,
        description="Pin a stable snapshot version for streaming dataset event migration",
    )
    dataset_events_use_seen_db: bool = Field(
        default=True,
        description="Use a SQLite seen-id store to prevent older versions overwriting newer ones during dataset pagination",
    )

    # Attachment migration (best-effort)
    copy_attachments: bool = Field(
        default=False,
        description="Copy Braintrust-managed attachments (type=braintrust_attachment) between orgs by downloading from source and re-uploading to destination",
    )
    attachment_max_bytes: int = Field(
        default=50 * 1024 * 1024,
        ge=1,
        le=2_000_000_000,
        description="Maximum size (bytes) of a single attachment to copy. Larger attachments will error/skip depending on migrator behavior.",
    )

    @field_validator("created_after")
    def validate_created_after(cls, v: str | None) -> str | None:
        if v is None:
            return None
        return canonicalize_created_after(v)


class LoggingConfig(BaseModel):
    """Configuration for structured logging."""

    level: str = Field(default="INFO", description="Log level")
    format: str = Field(default="json", description="Log format (json or text)")

    @field_validator("level")
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of: {', '.join(valid_levels)}")
        return v.upper()

    @field_validator("format")
    def validate_log_format(cls, v: str) -> str:
        """Validate log format."""
        if v.lower() not in {"json", "text"}:
            raise ValueError("Log format must be 'json' or 'text'")
        return v.lower()


class Config(BaseModel):
    """Main configuration class for the Braintrust migration tool."""

    source: BraintrustOrgConfig
    destination: BraintrustOrgConfig
    migration: MigrationConfig = Field(default_factory=MigrationConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    state_dir: Path = Field(
        default=Path("./checkpoints"),
        description="Directory for storing migration state and checkpoints",
    )
    resources: list[str] = Field(
        default=["all"], description="List of resources to migrate"
    )
    project_names: list[str] | None = Field(
        default=None,
        description="List of project names to migrate (if None, migrate all projects)",
    )

    class Config:
        """Pydantic config."""

        validate_assignment = True
        use_enum_values = True

    @classmethod
    def from_env(cls) -> "Config":
        """Create configuration from environment variables.

        Returns:
            Config instance populated from environment variables.

        Raises:
            ValueError: If required environment variables are missing.
        """
        # Required environment variables
        source_api_key = os.getenv("BT_SOURCE_API_KEY")
        dest_api_key = os.getenv("BT_DEST_API_KEY")

        if not source_api_key:
            raise ValueError("BT_SOURCE_API_KEY environment variable is required")
        if not dest_api_key:
            raise ValueError("BT_DEST_API_KEY environment variable is required")

        # Optional environment variables with defaults
        source_url = os.getenv("BT_SOURCE_URL", "https://api.braintrust.dev")
        dest_url = os.getenv("BT_DEST_URL", "https://api.braintrust.dev")

        # Migration settings
        batch_size = int(os.getenv("MIGRATION_BATCH_SIZE", "100"))
        retry_attempts = int(os.getenv("MIGRATION_RETRY_ATTEMPTS", "3"))
        retry_delay = float(os.getenv("MIGRATION_RETRY_DELAY", "1.0"))
        max_concurrent = int(os.getenv("MIGRATION_MAX_CONCURRENT", "10"))
        checkpoint_interval = int(os.getenv("MIGRATION_CHECKPOINT_INTERVAL", "50"))

        insert_max_request_bytes = int(
            os.getenv("MIGRATION_INSERT_MAX_REQUEST_BYTES", str(6 * 1024 * 1024))
        )
        insert_request_headroom_ratio = float(
            os.getenv("MIGRATION_INSERT_REQUEST_HEADROOM_RATIO", "0.5")
        )

        # Logs streaming settings
        # Keep defaults consistent with MigrationConfig (1000) so env-driven runs match
        # config-file/default-driven runs.
        logs_fetch_limit = int(os.getenv("MIGRATION_LOGS_FETCH_LIMIT", "1000"))
        logs_insert_batch_size = int(
            os.getenv("MIGRATION_LOGS_INSERT_BATCH_SIZE", "200")
        )
        logs_use_version_snapshot = os.getenv(
            "MIGRATION_LOGS_USE_VERSION_SNAPSHOT", "true"
        ).lower() in {"1", "true", "yes", "y", "on"}
        logs_use_seen_db = os.getenv("MIGRATION_LOGS_USE_SEEN_DB", "true").lower() in {
            "1",
            "true",
            "yes",
            "y",
            "on",
        }

        # Optional time filter (logs only)
        created_after = os.getenv("MIGRATION_CREATED_AFTER")

        # Experiment events streaming settings
        experiment_events_fetch_limit = int(
            os.getenv("MIGRATION_EXPERIMENT_EVENTS_FETCH_LIMIT", "1000")
        )
        experiment_events_insert_batch_size = int(
            os.getenv("MIGRATION_EXPERIMENT_EVENTS_INSERT_BATCH_SIZE", "200")
        )
        experiment_events_use_version_snapshot = os.getenv(
            "MIGRATION_EXPERIMENT_EVENTS_USE_VERSION_SNAPSHOT", "true"
        ).lower() in {"1", "true", "yes", "y", "on"}
        experiment_events_use_seen_db = os.getenv(
            "MIGRATION_EXPERIMENT_EVENTS_USE_SEEN_DB", "true"
        ).lower() in {"1", "true", "yes", "y", "on"}

        # Dataset events streaming settings
        dataset_events_fetch_limit = int(
            os.getenv("MIGRATION_DATASET_EVENTS_FETCH_LIMIT", "1000")
        )
        dataset_events_insert_batch_size = int(
            os.getenv("MIGRATION_DATASET_EVENTS_INSERT_BATCH_SIZE", "200")
        )
        dataset_events_use_version_snapshot = os.getenv(
            "MIGRATION_DATASET_EVENTS_USE_VERSION_SNAPSHOT", "true"
        ).lower() in {"1", "true", "yes", "y", "on"}
        dataset_events_use_seen_db = os.getenv(
            "MIGRATION_DATASET_EVENTS_USE_SEEN_DB", "true"
        ).lower() in {"1", "true", "yes", "y", "on"}

        # Attachment copy settings
        copy_attachments = os.getenv("MIGRATION_COPY_ATTACHMENTS", "false").lower() in {
            "1",
            "true",
            "yes",
            "y",
            "on",
        }
        attachment_max_bytes = int(
            os.getenv("MIGRATION_ATTACHMENT_MAX_BYTES", str(50 * 1024 * 1024))
        )

        # Logging settings
        log_level = os.getenv("LOG_LEVEL", "INFO")
        log_format = os.getenv("LOG_FORMAT", "json")

        # State directory
        state_dir = Path(os.getenv("MIGRATION_STATE_DIR", "./checkpoints"))

        return cls(
            source=BraintrustOrgConfig(
                api_key=source_api_key,
                url=cast(HttpUrl, HttpUrl(source_url)),
            ),
            destination=BraintrustOrgConfig(
                api_key=dest_api_key,
                url=cast(HttpUrl, HttpUrl(dest_url)),
            ),
            migration=MigrationConfig(
                batch_size=batch_size,
                retry_attempts=retry_attempts,
                retry_delay=retry_delay,
                max_concurrent=max_concurrent,
                checkpoint_interval=checkpoint_interval,
                insert_max_request_bytes=insert_max_request_bytes,
                insert_request_headroom_ratio=insert_request_headroom_ratio,
                logs_fetch_limit=logs_fetch_limit,
                logs_insert_batch_size=logs_insert_batch_size,
                logs_use_version_snapshot=logs_use_version_snapshot,
                logs_use_seen_db=logs_use_seen_db,
                created_after=created_after,
                experiment_events_fetch_limit=experiment_events_fetch_limit,
                experiment_events_insert_batch_size=experiment_events_insert_batch_size,
                experiment_events_use_version_snapshot=experiment_events_use_version_snapshot,
                experiment_events_use_seen_db=experiment_events_use_seen_db,
                dataset_events_fetch_limit=dataset_events_fetch_limit,
                dataset_events_insert_batch_size=dataset_events_insert_batch_size,
                dataset_events_use_version_snapshot=dataset_events_use_version_snapshot,
                dataset_events_use_seen_db=dataset_events_use_seen_db,
                copy_attachments=copy_attachments,
                attachment_max_bytes=attachment_max_bytes,
            ),
            logging=LoggingConfig(
                level=log_level,
                format=log_format,
            ),
            state_dir=state_dir,
        )

    @classmethod
    def from_file(cls, config_path: Path) -> "Config":
        """Create configuration from a YAML or JSON file.

        Args:
            config_path: Path to the configuration file

        Returns:
            Config instance populated from the file

        Raises:
            ValueError: If the file format is unsupported or required fields are missing
            FileNotFoundError: If the configuration file doesn't exist
        """
        import json

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        file_extension = config_path.suffix.lower()

        try:
            if file_extension == ".json":
                with open(config_path) as f:
                    config_data = json.load(f)
            elif file_extension in [".yaml", ".yml"]:
                try:
                    import yaml
                except ImportError:
                    raise ValueError(
                        "PyYAML is required to load YAML configuration files. Install with: pip install PyYAML"
                    ) from None

                with open(config_path) as f:
                    config_data = yaml.safe_load(f)
            else:
                raise ValueError(
                    f"Unsupported configuration file format: {file_extension}. Supported formats: .json, .yaml, .yml"
                )

            # Validate and create Config instance
            return cls(**config_data)

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}") from e
        except Exception as e:
            raise ValueError(f"Failed to load configuration file: {e}") from e

    def get_checkpoint_dir(self, project_name: str | None = None) -> Path:
        """Get the checkpoint directory for a specific project or general use.

        Args:
            project_name: Optional project name for project-specific checkpoints.

        Returns:
            Path to the checkpoint directory.
        """
        if project_name:
            return self.state_dir / project_name
        return self.state_dir

    def ensure_checkpoint_dir(self, project_name: str | None = None) -> Path:
        """Ensure checkpoint directory exists and return the path.

        Args:
            project_name: Optional project name for project-specific checkpoints.

        Returns:
            Path to the checkpoint directory.
        """
        checkpoint_dir = self.get_checkpoint_dir(project_name)
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        return checkpoint_dir
