#!/usr/bin/env python3
"""Weekly OpenAPI spec diff + LLM summary helper."""

from __future__ import annotations

import argparse
import json
import os
import sys
import textwrap
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from deepdiff import DeepDiff

DEFAULT_SPEC_URL = "https://raw.githubusercontent.com/braintrustdata/braintrust-openapi/refs/heads/main/openapi/spec.json"
OPENAI_MODEL = "gpt-4.1"
SUMMARY_MAX_CHARS = 3500
OPENAI_TIMEOUT_SECS = 60
OPENAI_MAX_RETRIES = 3
OPENAI_RETRY_BACKOFF_SECS = 2

# ---------------------------------------------------------------------------
# Migration tool API usage context
# ---------------------------------------------------------------------------
# This tells the LLM exactly how the migration tool uses the Braintrust API
# so it can accurately assess whether a spec change actually impacts the tool.
# Update this when adding new migrators or changing API usage patterns.
# ---------------------------------------------------------------------------

TOOL_USAGE_CONTEXT = """
## How the migration tool uses the Braintrust API

### Endpoint usage

The tool uses these REST endpoints:
- GET  /v1/{resource}  — list resources (datasets, experiments, functions, prompts, etc.)
  Always passes explicit `limit=1000` and paginates via `starting_after`. Never relies on server default limit.
- POST /v1/{resource}  — create resources (one at a time)
- POST /v1/project     — create/get project (only sends `name` and `description`)
- POST /btql           — BTQL queries for streaming event pagination (logs, dataset events, experiment events)
- POST /v1/project_logs/{id}/insert   — bulk insert log events
- POST /v1/dataset/{id}/insert        — bulk insert dataset events
- POST /v1/experiment/{id}/insert     — bulk insert experiment events

### Schema usage (what matters for create payloads)

The tool uses `openapi_utils.get_resource_create_fields()` to read the **top-level property names** from these Create* schemas and uses them as a field-name allowlist:
- CreateDataset, CreateExperiment, CreateFunction, CreatePrompt
- CreateProjectScore, CreateProjectTag, CreateView, CreateGroup, CreateRole
- CreateAISecret, CreateSpanIframe, CreateProjectAutomation

This is a **field-name filter only** — it checks which keys to include in the POST body.
It does NOT validate field values, nested object structure, enum values, or required-ness.

### What the tool does NOT use

- ProjectSettings, remote_eval_sources — never read or written. Project creation only uses name + description.
- TopicMapData — nested inside function_data; passed through as an opaque blob from source to destination.
- Patch* / Update* schemas — the tool only creates, never updates.
- Any enum validation — enum values are passed through verbatim from source to destination.
- Server-side default for `limit` parameter — always sends explicit limit=1000.

### Streaming events (logs, datasets, experiments)

Events are copied via BTQL sorted pagination (sorted by `_pagination_key`).
Insert payloads are filtered using InsertDatasetEvent / InsertExperimentEvent / InsertProjectLogsEvent
schemas (top-level field names only). Nested fields (span_attributes, metadata, etc.) pass through as-is.

### General patterns

- Source data is treated as opaque — the tool reads from source and writes to destination with minimal transformation.
- Only field-name filtering is applied (via OpenAPI Create* schemas). No value validation.
- ID remapping is done for foreign keys (project_id, dataset_id, base_exp_id, function references).
- The tool handles HTTP 413 (payload too large) by bisecting batches.
- The tool handles HTTP 429 (rate limit) with exponential backoff and Retry-After.
""".strip()



def _fetch_json(url: str) -> tuple[dict[str, Any], bytes]:
    with urllib.request.urlopen(url, timeout=20) as response:
        raw = response.read()
    return json.loads(raw.decode()), raw


def _load_local_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def _truncate_value(value: Any, limit: int = 160) -> str:
    rendered = json.dumps(value, ensure_ascii=True)
    if len(rendered) <= limit:
        return rendered
    return f"{rendered[: limit - 3]}..."


def _collect_paths(section: Any) -> list[str]:
    if isinstance(section, dict):
        return list(section.keys())
    if isinstance(section, list | set | tuple):
        return list(section)
    return []


def _build_diff_summary(diff: DeepDiff, max_items: int = 200) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "added": [],
        "removed": [],
        "values_changed": [],
        "type_changes": [],
        "iterable_added": [],
        "iterable_removed": [],
    }

    summary["added"] = sorted(_collect_paths(diff.get("dictionary_item_added", [])))
    summary["removed"] = sorted(_collect_paths(diff.get("dictionary_item_removed", [])))
    summary["iterable_added"] = sorted(
        _collect_paths(diff.get("iterable_item_added", {}))
    )
    summary["iterable_removed"] = sorted(
        _collect_paths(diff.get("iterable_item_removed", {}))
    )

    for path, change in diff.get("values_changed", {}).items():
        summary["values_changed"].append(
            {
                "path": path,
                "old": _truncate_value(change.get("old_value")),
                "new": _truncate_value(change.get("new_value")),
            }
        )

    for path, change in diff.get("type_changes", {}).items():
        summary["type_changes"].append(
            {
                "path": path,
                "old_type": str(change.get("old_type")),
                "new_type": str(change.get("new_type")),
            }
        )

    for key in ("added", "removed", "iterable_added", "iterable_removed"):
        if len(summary[key]) > max_items:
            summary[key] = summary[key][:max_items] + [
                f"... {len(summary[key]) - max_items} more"
            ]

    for key in ("values_changed", "type_changes"):
        if len(summary[key]) > max_items:
            summary[key] = summary[key][:max_items] + [
                {"path": f"... {len(summary[key]) - max_items} more"}
            ]

    return summary


def _format_for_prompt(summary: dict[str, Any]) -> str:
    return json.dumps(summary, ensure_ascii=True, indent=2)


def _call_openai(prompt: str, model: str) -> str:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You assess OpenAPI spec diffs for maintainers of a Braintrust migration tool. "
                    "You have detailed context about exactly how the tool uses the API. "
                    "Only flag changes that ACTUALLY affect the tool based on its real usage patterns. "
                    "Be precise: say 'NO IMPACT' when the tool doesn't use the affected schema/endpoint."
                ),
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        "temperature": 0,
    }

    data = json.dumps(payload).encode()
    request = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
    )
    last_error: Exception | None = None
    for attempt in range(1, OPENAI_MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(
                request, timeout=OPENAI_TIMEOUT_SECS
            ) as response:
                body = json.loads(response.read().decode())
            return body["choices"][0]["message"]["content"].strip()
        except (TimeoutError, urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt == OPENAI_MAX_RETRIES:
                break
            time.sleep(OPENAI_RETRY_BACKOFF_SECS * attempt)

    raise RuntimeError(
        f"OpenAI request failed after {OPENAI_MAX_RETRIES} attempts: {last_error}"
    ) from last_error


def _fallback_summary(summary: dict[str, Any]) -> str:
    parts = [
        "OpenAPI spec changed, but LLM summary was unavailable.",
        f"Added paths: {len(summary.get('added', []))}",
        f"Removed paths: {len(summary.get('removed', []))}",
        f"Value changes: {len(summary.get('values_changed', []))}",
        f"Type changes: {len(summary.get('type_changes', []))}",
    ]
    return "\n".join(parts)


def _write_outputs(output_path: Path, changed: bool, summary: str) -> None:
    if not output_path:
        return
    with output_path.open("a") as handle:
        handle.write(f"changed={'true' if changed else 'false'}\n")
        handle.write("summary<<EOF\n")
        handle.write(summary.rstrip() + "\n")
        handle.write("EOF\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Weekly OpenAPI spec diff helper.")
    parser.add_argument("--spec-url", default=DEFAULT_SPEC_URL)
    parser.add_argument("--local-path", default="openapi_spec.json")
    parser.add_argument("--output", default=os.environ.get("GITHUB_OUTPUT"))
    parser.add_argument("--write-updated", action="store_true")
    args = parser.parse_args()

    try:
        latest_spec, latest_raw = _fetch_json(args.spec_url)
    except (urllib.error.URLError, json.JSONDecodeError) as exc:
        print(f"Failed to fetch spec: {exc}", file=sys.stderr)
        return 1

    local_path = Path(args.local_path)
    current_spec = _load_local_json(local_path)

    changed = current_spec != latest_spec
    summary_text = "No changes detected in Braintrust OpenAPI spec."

    if changed:
        diff = DeepDiff(current_spec, latest_spec, ignore_order=True)
        diff_summary = _build_diff_summary(diff)
        prompt = textwrap.dedent(
            f"""
            You are assessing OpenAPI spec changes for a Braintrust **migration tool**.
            Your job is to determine whether each change ACTUALLY impacts the tool,
            not whether it COULD impact a generic API consumer.

            {TOOL_USAGE_CONTEXT}

            ---

            Given the above context about how the tool uses the API, summarize the
            following spec diff. For each change, explicitly state whether it impacts
            the migration tool and why/why not.

            Classification guide:
            - **IMPACTS TOOL**: Change affects a Create* schema the tool uses, an endpoint
              the tool calls, or a field the tool reads/writes. Requires code changes.
            - **NO IMPACT**: Change is to a schema/field/endpoint the tool doesn't use,
              or is a relaxation (removed required field, added optional field, enum expansion)
              that makes the API more permissive.
            - **MONITOR**: Change doesn't break anything today but could matter if the tool
              adds support for that resource/field in the future.

            Format your response as:

            ### Impact Assessment
            For each notable change: one line with [IMPACTS TOOL / NO IMPACT / MONITOR],
            the change description, and a brief reason.

            ### New/Deleted Resources
            Any completely new or removed API endpoints or top-level schemas.

            ### Action Items
            Only list items classified as IMPACTS TOOL. If none, say "No action required."

            Diff summary (JSON paths, truncated):
            {_format_for_prompt(diff_summary)}
            """
        ).strip()

        summary_text = _call_openai(prompt, OPENAI_MODEL)

        if len(summary_text) > SUMMARY_MAX_CHARS:
            summary_text = summary_text[: SUMMARY_MAX_CHARS - 3] + "..."

        if args.write_updated:
            local_path.write_bytes(latest_raw)

    output_path = Path(args.output) if args.output else None
    if output_path:
        _write_outputs(output_path, changed, summary_text)

    print(summary_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
