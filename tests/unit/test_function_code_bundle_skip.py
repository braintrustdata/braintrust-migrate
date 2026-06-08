"""FunctionMigrator skips bundle-backed code functions (not migratable).

A code function's bundle is built by the push/eval pipeline and isn't
retrievable via the API, so it can't be recreated in the destination. Inline
code functions carry their source and migrate fine.
"""

from __future__ import annotations

import pytest

from braintrust_migrate.resources.functions import (
    CODE_BUNDLE_SKIP_REASON,
    FunctionMigrator,
)


def _bundle_fn(fid: str = "f-bundle") -> dict:
    return {
        "id": fid,
        "name": "bundle scorer",
        "slug": "bundle-scorer",
        "project_id": "p",
        "function_type": "scorer",
        "function_data": {
            "type": "code",
            "data": {
                "type": "bundle",
                "bundle_id": "b1",
                "runtime_context": {"runtime": "node", "version": "20"},
                "location": {
                    "type": "experiment",
                    "eval_name": "e",
                    "position": {"type": "scorer", "index": 0},
                },
            },
        },
    }


def _inline_fn(fid: str = "f-inline") -> dict:
    return {
        "id": fid,
        "name": "inline scorer",
        "slug": "inline-scorer",
        "project_id": "p",
        "function_type": "scorer",
        "function_data": {
            "type": "code",
            "data": {
                "type": "inline",
                "code": "def handler(): return 1",
                "runtime_context": {"runtime": "python", "version": "3.11"},
            },
        },
    }


def _prompt_fn(fid: str = "f-prompt") -> dict:
    return {
        "id": fid,
        "name": "prompt fn",
        "slug": "prompt-fn",
        "project_id": "p",
        "function_type": "llm",
        "function_data": {"type": "prompt"},
    }


def test_is_code_bundle_function_detection():
    assert FunctionMigrator._is_code_bundle_function(_bundle_fn()) is True
    assert FunctionMigrator._is_code_bundle_function(_inline_fn()) is False
    assert FunctionMigrator._is_code_bundle_function(_prompt_fn()) is False
    # sandbox / missing data on a code function -> not inline -> skip.
    assert (
        FunctionMigrator._is_code_bundle_function(
            {"function_data": {"type": "code", "data": {"type": "sandbox"}}}
        )
        is True
    )
    assert (
        FunctionMigrator._is_code_bundle_function({"function_data": {"type": "code"}})
        is True
    )
    # non-code / no function_data -> never skipped here.
    assert FunctionMigrator._is_code_bundle_function({}) is False
    assert (
        FunctionMigrator._is_code_bundle_function(
            {"function_data": {"type": "global"}}
        )
        is False
    )


@pytest.mark.asyncio
async def test_migrate_batch_skips_bundle_keeps_inline_and_prompt(
    mock_source_client, mock_dest_client, temp_checkpoint_dir
):
    migrator = FunctionMigrator(
        mock_source_client, mock_dest_client, temp_checkpoint_dir
    )
    migrator.dest_project_id = "dest-project"

    async def mock_with_retry(operation_name, coro_func, **kwargs):
        result = coro_func()
        return await result if hasattr(result, "__await__") else result

    mock_dest_client.with_retry.side_effect = mock_with_retry

    created: list[dict] = []

    async def mock_raw_request(method, path, *, json=None, **kwargs):
        created.append(json)
        return {"id": f"dest-{json.get('slug')}", "name": json.get("name")}

    mock_dest_client.raw_request.side_effect = mock_raw_request

    results = await migrator.migrate_batch([_bundle_fn(), _inline_fn(), _prompt_fn()])
    by_source = {r.source_id: r for r in results}

    # Bundle function: skipped with the documented reason; never sent to dest.
    assert by_source["f-bundle"].skipped is True
    assert by_source["f-bundle"].metadata["skip_reason"] == CODE_BUNDLE_SKIP_REASON

    # Inline + prompt: migrated normally.
    assert by_source["f-inline"].skipped is False
    assert by_source["f-inline"].success is True
    assert by_source["f-prompt"].skipped is False
    assert by_source["f-prompt"].success is True

    # Exactly the two migratable functions hit the create endpoint; no bundle
    # function_data was ever sent.
    assert len(created) == 2
    assert {c.get("slug") for c in created} == {"inline-scorer", "prompt-fn"}
    for c in created:
        assert c.get("function_data", {}).get("data", {}).get("type") != "bundle"
