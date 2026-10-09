"""Project settings migration for Braintrust migration tool.

Project settings (``Project.settings``) can't be set on create, and some of them
reference other resources by id (the default preprocessor function and the
baseline experiment). They are therefore applied with a single
``PATCH /v1/project/{id}`` after the project's resources have been migrated,
using the accumulated source -> destination id mapping.

Settings already set on the destination project are kept, matching how the
tool treats existing destination projects (additive, never replacing).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

# Settings copied verbatim: they hold no resource ids.
PASSTHROUGH_SETTINGS = (
    "comparison_key",
    "spanFieldOrder",
    "remote_eval_sources",
    "disable_realtime_queries",
    "blind_reviews",
    "require_all_human_review_scores",
    "coding_agent_insights_dashboard",
    "monitor_charts_use_metrics_start",
)


@dataclass
class SettingsPlan:
    """Outcome of merging source settings into destination settings."""

    settings: dict[str, Any]
    applied: list[str] = field(default_factory=list)
    kept_existing: list[str] = field(default_factory=list)
    skipped: dict[str, str] = field(default_factory=dict)

    @property
    def has_changes(self) -> bool:
        return bool(self.applied)


def _remap_default_preprocessor(
    value: dict[str, Any], id_mapping: dict[str, str]
) -> tuple[dict[str, Any] | None, str | None]:
    """Map a SavedFunctionId to the destination; returns (value, skip_reason)."""
    if value.get("type") == "global":
        return value, None
    if value.get("type") == "function":
        dest_id = id_mapping.get(value.get("id", ""))
        if not dest_id:
            return None, "preprocessor_function_not_migrated"
        # The source version is a source-org transaction id; use the latest
        # destination version instead.
        return {"type": "function", "id": dest_id}, None
    return None, "unsupported_preprocessor_reference"


def plan_project_settings(
    source_settings: dict[str, Any] | None,
    dest_settings: dict[str, Any] | None,
    id_mapping: dict[str, str],
) -> SettingsPlan:
    """Merge source project settings into the destination's current settings.

    Args:
        source_settings: ``settings`` of the source project.
        dest_settings: Current ``settings`` of the destination project.
        id_mapping: Source -> destination resource id mapping.

    Returns:
        The full settings object to PATCH (PATCH replaces all settings) and a
        per-key account of what was applied, kept, or skipped.
    """
    merged = dict(dest_settings or {})
    plan = SettingsPlan(settings=merged)

    for key, value in (source_settings or {}).items():
        if value is None:
            continue
        if merged.get(key) is not None:
            plan.kept_existing.append(key)
            continue

        if key in PASSTHROUGH_SETTINGS:
            new_value: Any = value
        elif key == "baseline_experiment_id":
            new_value = id_mapping.get(value)
            if not new_value:
                plan.skipped[key] = "baseline_experiment_not_migrated"
                continue
        elif key == "default_preprocessor" and isinstance(value, dict):
            new_value, reason = _remap_default_preprocessor(value, id_mapping)
            if reason:
                plan.skipped[key] = reason
                continue
        else:
            # Unknown settings may embed source ids; don't copy them blindly.
            plan.skipped[key] = "unknown_setting"
            continue

        merged[key] = new_value
        plan.applied.append(key)

    return plan


async def migrate_project_settings(
    *,
    source_settings: dict[str, Any] | None,
    dest_client: Any,
    dest_project_id: str,
    id_mapping: dict[str, str],
    project_name: str,
) -> dict[str, Any]:
    """Apply source project settings to the destination project.

    Returns a results dict shaped like the resource migrators' results, where
    each source setting counts as one resource.
    """
    log = logger.bind(project=project_name, dest_project_id=dest_project_id)
    present = [k for k, v in (source_settings or {}).items() if v is not None]
    results: dict[str, Any] = {
        "total": len(present),
        "migrated": 0,
        "skipped": 0,
        "failed": 0,
        "errors": [],
    }
    if not present:
        return results

    try:
        dest_project = await dest_client.with_retry(
            "get_dest_project",
            lambda: dest_client.raw_request("GET", f"/v1/project/{dest_project_id}"),
        )
        plan = plan_project_settings(
            source_settings, dest_project.get("settings"), id_mapping
        )

        if plan.has_changes:
            await dest_client.with_retry(
                "update_project_settings",
                lambda: dest_client.raw_request(
                    "PATCH",
                    f"/v1/project/{dest_project_id}",
                    json={"settings": plan.settings},
                ),
            )
            log.info("Migrated project settings", applied=plan.applied)

        if plan.kept_existing:
            log.warning(
                "Kept existing destination project settings (not overwritten)",
                settings=plan.kept_existing,
            )
        for key, reason in plan.skipped.items():
            log.warning("Skipped project setting", setting=key, skip_reason=reason)

        skip_reasons = {k: "dest_already_set" for k in plan.kept_existing}
        skip_reasons.update(plan.skipped)
        results["migrated"] = len(plan.applied)
        results["skipped"] = len(skip_reasons)
        if skip_reasons:
            # source_id/name are what the migration report expects of every
            # skipped item; a setting has no id of its own, so use its key.
            results["skipped_details"] = [
                {"setting": k, "source_id": k, "name": None, "skip_reason": r}
                for k, r in skip_reasons.items()
            ]
            counts: dict[str, int] = {}
            for reason in skip_reasons.values():
                counts[reason] = counts.get(reason, 0) + 1
            results["skip_summary"] = ", ".join(f"{n} {r}" for r, n in counts.items())
    except Exception as e:
        log.error("Failed to migrate project settings", error=str(e))
        results["failed"] = len(present)
        results["errors"].append(
            {
                "resource_type": "project_settings",
                "error": f"Failed to migrate project settings: {e}",
                "error_type": type(e).__name__,
            }
        )

    return results
