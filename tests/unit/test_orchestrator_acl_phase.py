"""Unit tests for ACL phase placement in migration orchestration."""

from pydantic import HttpUrl

from braintrust_migrate.config import BraintrustOrgConfig, Config, MigrationConfig
from braintrust_migrate.orchestration import MigrationOrchestrator


def _make_config(resources: list[str]) -> Config:
    return Config(
        source=BraintrustOrgConfig(
            api_key="source-key",
            url=HttpUrl("https://source.braintrust.dev"),
        ),
        destination=BraintrustOrgConfig(
            api_key="dest-key",
            url=HttpUrl("https://dest.braintrust.dev"),
        ),
        migration=MigrationConfig(),
        resources=resources,
    )


def test_acl_is_in_post_project_phase() -> None:
    config = _make_config(["all"])
    orchestrator = MigrationOrchestrator(config)

    assert ("acls",) == (
        orchestrator.POST_PROJECT_GLOBAL_RESOURCES[0][0],
    )
    assert orchestrator.MIGRATION_ORDER[-1][0] == "acls"


def test_post_project_resource_filtering_honors_explicit_selection() -> None:
    config = _make_config(["acls"])
    orchestrator = MigrationOrchestrator(config)

    assert orchestrator._get_post_project_resources_to_migrate() == ["acls"]
