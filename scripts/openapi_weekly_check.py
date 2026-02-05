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
                    "You summarize OpenAPI spec diffs for maintainers of a migration tool. "
                    "Focus on additions/removals/updates that impact migration resources, "
                    "schema names, and create/update payload fields. Provide a brief impact "
                    "assessment and recommended follow-ups."
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
            Summarize changes between the previous and latest Braintrust OpenAPI spec.

            Focus on:
            - Completely new or deleted resources, endpoints, or schemas.
            - New required fields or breaking changes for create/update payloads.
            - Changes that could cause migration failures or data loss.

            De-emphasize:
            - Enum expansions or relaxed optionality unless we validate values (we don't).
            - Purely additive optional fields unless they affect creation.

            Provide:
            - A short list of top-impact changes (most important first).
            - Explicit callout of new/deleted resources.
            - Recommended follow-ups if any (tests or code updates).

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
