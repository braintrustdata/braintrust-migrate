"""Unit tests for custom Logs column migration; all API calls are mocked."""

import pytest

from braintrust_migrate.resources.columns import ColumnMigrator


def column(
    project="source", name="Segment", expr="metadata.segment", id="source-column"
):
    return dict(
        id=id,
        object_type="project",
        object_id=project,
        subtype="project_log",
        variant="project_log",
        name=name,
        expr=expr,
        created="2026-01-01T00:00:00Z",
    )


@pytest.fixture
def migrator(mock_source_client, mock_dest_client, temp_checkpoint_dir):
    result = ColumnMigrator(mock_source_client, mock_dest_client, temp_checkpoint_dir)
    result.set_destination_project_id("destination")
    mock_source_client.raw_request.return_value = {"objects": [column()]}
    return result


@pytest.mark.asyncio
async def test_create_filters_fields_and_verifies(migrator):
    dest = column("destination", id="dest-column")
    migrator.dest_client.raw_request.side_effect = [
        {"objects": []},
        dest,
        {"objects": [dest]},
    ]
    summary = await migrator.migrate_all("source")
    assert summary["migrated"] == 1
    calls = migrator.dest_client.raw_request.call_args_list
    assert calls[1].args == ("POST", "/v1/column")
    assert calls[1].kwargs["json"] == {
        k: v for k, v in dest.items() if k not in {"id", "created"}
    }
    assert calls[2].args == ("GET", "/v1/column")
    params = migrator.source_client.raw_request.call_args.kwargs["params"]
    assert params == dict(
        object_type="project",
        object_id="source",
        subtype="project_log",
        variant="project_log",
    )


@pytest.mark.asyncio
async def test_existing_match_skipped(migrator):
    dest = column("destination", id="dest-column")
    migrator.dest_client.raw_request.return_value = {"objects": [dest]}
    summary = await migrator.migrate_all("source")
    assert summary["skipped"] == 1
    assert summary["migrated"] == 0
    assert all(
        c.args[0] == "GET" for c in migrator.dest_client.raw_request.call_args_list
    )


@pytest.mark.asyncio
async def test_conflict_prevents_all_column_writes(migrator):
    migrator.source_client.raw_request.return_value = {
        "objects": [
            column(name="Missing", id="missing"),
            column(),
        ]
    }
    migrator.dest_client.raw_request.return_value = {
        "objects": [column("destination", expr="metadata.other")]
    }
    with pytest.raises(ValueError, match="expression conflicts"):
        await migrator.migrate_all("source")
    assert migrator.dest_client.raw_request.call_count == 1


@pytest.mark.asyncio
async def test_verification_detects_missing_column(migrator):
    migrator.dest_client.raw_request.side_effect = [
        {"objects": []},
        column("destination"),
        {"objects": []},
    ]
    with pytest.raises(ValueError, match="verification failed"):
        await migrator.migrate_all("source")
    assert "source-column" not in migrator.state.id_mapping
    assert "source-column" in migrator.state.failed_ids


@pytest.mark.asyncio
async def test_stale_checkpoint_does_not_skip_missing_column(migrator):
    migrator.state.id_mapping["source-column"] = "deleted-column"
    dest = column("destination", id="new-column")
    migrator.dest_client.raw_request.side_effect = [
        {"objects": []},
        dest,
        {"objects": [dest]},
    ]
    summary = await migrator.migrate_all("source")
    assert summary["migrated"] == 1
    assert migrator.state.id_mapping["source-column"] == "new-column"


@pytest.mark.asyncio
@pytest.mark.parametrize("objects", [[column("wrong")], [column(), column()], None])
async def test_invalid_response_stops_before_writes(migrator, objects):
    migrator.source_client.raw_request.return_value = {"objects": objects}
    with pytest.raises(ValueError, match="Invalid|Duplicate"):
        await migrator.migrate_all("source")
    migrator.dest_client.raw_request.assert_not_called()


@pytest.mark.asyncio
async def test_post_conflict_race_not_recorded_as_success(migrator):
    migrator.dest_client.raw_request.return_value = column("destination", expr="other")
    with pytest.raises(ValueError, match="does not match"):
        await migrator.migrate_resource(column())


@pytest.mark.asyncio
async def test_missing_project_rejected(migrator):
    with pytest.raises(ValueError, match="source project ID"):
        await migrator.list_source_resources()
    migrator.dest_project_id = None
    with pytest.raises(ValueError, match="destination project ID"):
        await migrator.migrate_all("source")
    migrator.dest_client.raw_request.assert_not_called()


@pytest.mark.asyncio
async def test_partial_failure_preserves_successful_mapping(migrator):
    migrator.source_client.raw_request.return_value = {
        "objects": [
            column(),
            column(name="Other", id="other-column"),
        ]
    }
    dest = column("destination", id="dest-column")
    migrator.dest_client.raw_request.side_effect = [
        {"objects": []},
        dest,
        RuntimeError("create failed"),
        {"objects": [dest]},
    ]
    with pytest.raises(ValueError, match="verification failed"):
        await migrator.migrate_all("source", max_concurrent=1)
    assert migrator.state.id_mapping["source-column"] == "dest-column"
    assert "other-column" in migrator.state.failed_ids


@pytest.mark.asyncio
@pytest.mark.parametrize("create_fails", [True, False])
async def test_orchestrator_reports_partial_column_failure(
    migrator, tmp_path, create_fails
):
    from braintrust_migrate.config import Config
    from braintrust_migrate.orchestration import MigrationOrchestrator

    migrator.source_client.raw_request.return_value = {
        "objects": [
            column(),
            column(name="Other", id="other-column"),
        ]
    }
    dest = column("destination", id="dest-column")
    migrator.dest_client.raw_request.side_effect = [
        {"objects": []},
        dest,
        RuntimeError("create failed")
        if create_fails
        else column("destination", name="Other", id="other-dest"),
        {"objects": [dest]},
    ]
    config = Config(
        source={"api_key": "test-source"},
        destination={"api_key": "test-destination"},
        resources=["columns"],
        state_dir=tmp_path,
        migration={"max_concurrent_resources": 1},
    )
    report = await MigrationOrchestrator(config)._migrate_project(
        {"name": "Test", "source_id": "source", "dest_id": "destination"},
        migrator.source_client,
        migrator.dest_client,
        tmp_path,
        {},
    )
    result = report["resources"]["columns"]
    assert result["total"] == result["migrated"] + result["failed"]
    assert result["migrated"] == 1
    assert result["failed"] == 1
    assert result["skipped"] == 0
    assert report["migrated_resources"] == 1
    assert report["failed_resources"] == 1
    assert result["errors"][0]["source_id"] == "other-column"
