"""Utilities for working with OpenAPI specifications to determine valid create fields."""

import json
import logging
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Official Braintrust OpenAPI spec URL
BRAINTRUST_OPENAPI_URL = "https://raw.githubusercontent.com/braintrustdata/braintrust-openapi/refs/heads/main/openapi/spec.json"


class OpenAPISchemaManager:
    """Manage OpenAPI schema data to determine valid create fields."""

    def __init__(
        self,
        spec_url: str = BRAINTRUST_OPENAPI_URL,
        local_fallback_path: str | Path | None = None,
    ):
        """Initialize with OpenAPI spec source.

        Args:
            spec_url: URL to fetch the OpenAPI spec from
            local_fallback_path: Local file to use if URL fetch fails. If None, uses project openapi_spec.json
        """
        self.spec_url = spec_url

        if local_fallback_path is None:
            local_fallback_path = Path(__file__).parent.parent / "openapi_spec.json"
        self.local_fallback_path = Path(local_fallback_path)

        self._spec_data: dict[str, Any] | None = None

    @property
    def spec_data(self) -> dict[str, Any]:
        """Lazily load and cache the OpenAPI spec data."""
        if self._spec_data is None:
            self._spec_data = self._load_spec()
        return self._spec_data

    def _load_spec(self) -> dict[str, Any]:
        """Load OpenAPI spec, trying URL first, then local fallback."""
        # Try to fetch from URL first
        try:
            logger.debug(f"Fetching OpenAPI spec from {self.spec_url}")
            with urllib.request.urlopen(self.spec_url, timeout=10) as response:
                spec_data = json.loads(response.read().decode())
                logger.info(f"Successfully fetched OpenAPI spec from {self.spec_url}")
                return spec_data
        except (
            urllib.error.URLError,
            urllib.error.HTTPError,
            json.JSONDecodeError,
            TimeoutError,
        ) as e:
            logger.warning(
                f"Failed to fetch OpenAPI spec from URL: {e}, trying local fallback"
            )

        # Fallback to local file
        try:
            with open(self.local_fallback_path) as f:
                spec_data = json.load(f)
            logger.info(
                f"Loaded OpenAPI spec from local file: {self.local_fallback_path}"
            )
            return spec_data
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load local OpenAPI spec: {e}")
            return {}

    def get_resource_create_fields(self, resource_type: str) -> set[str] | None:
        """Get fields that are allowed for creating a resource.

        Args:
            resource_type: Resource type name (e.g., "Prompt", "Dataset", "Experiment")

        Returns:
            Set of field names that are allowed in create operations, or None if schema not found
        """
        if not self.spec_data:
            logger.warning("No OpenAPI spec data available")
            return None

        # Handle special cases where OpenAPI schema names don't follow Create{ResourceType} pattern
        special_cases = {
            "Log": "InsertProjectLogsEvent",
            "ExperimentEvent": "InsertExperimentEvent",
            "DatasetEvent": "InsertDatasetEvent",
            "ACL": "AclItem",
        }

        create_schema_name = special_cases.get(resource_type, f"Create{resource_type}")
        schemas = self.spec_data.get("components", {}).get("schemas", {})
        schema = schemas.get(create_schema_name, {})

        if not schema:
            logger.warning(f"Schema {create_schema_name} not found in OpenAPI spec")
            return None

        properties = schema.get("properties", {})
        allowed_fields = set(properties.keys())

        logger.debug(
            f"Found {len(allowed_fields)} allowed fields for {create_schema_name}: {allowed_fields}"
        )

        return allowed_fields


def get_resource_create_fields(
    resource_type: str, _cache: dict = {}
) -> set[str] | None:
    """Get create fields for a resource type.

    Args:
        resource_type: Resource type name (e.g., "Prompt", "Dataset", "Experiment")

    Returns:
        Set of field names that are allowed in create operations, or None if schema not found
    """
    if "manager" not in _cache:
        _cache["manager"] = OpenAPISchemaManager()
    return _cache["manager"].get_resource_create_fields(resource_type)
