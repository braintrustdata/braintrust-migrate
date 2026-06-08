"""Post-migration validation: confirm what was migrated exists in the destination.

Two check kinds, deliberately asymmetric (see the team discussion):

- **Object parity** — for project-scoped object resources (datasets, experiments,
  prompts, functions, project_scores, views, project_tags, span_iframes): list
  source vs dest, compare, and report the *specific* items missing in the
  destination. Cheap (small N, list endpoints).

- **Event count-parity** — for high-volume event resources (dataset events,
  experiment events, logs): compare a cheap ``count`` of source vs dest. We do
  NOT enumerate *which* events are missing — that would require materializing and
  diffing both full id sets (≈ re-reading the whole migration) and does not scale.
  Counts are reported per parent object (per dataset / experiment / project) so a
  discrepancy can still be localized to a specific object.

Validation mirrors migration policy (e.g. it excludes bundle-backed code
functions, which are intentionally skipped) so deliberate skips are not reported
as missing.

Org-scoped resources (roles, groups, ai_secrets) and ACLs are intentionally out
of scope for now.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from braintrust_migrate.btql import btql_quote
from braintrust_migrate.resources.functions import FunctionMigrator

# Lists resources of a type from a client for a project: (client, type, project_id).
ListFn = Callable[[Any, str, str | None], Awaitable[list[dict[str, Any]]]]


@dataclass
class CheckResult:
    kind: str  # "object" | "events"
    scope: str  # e.g. "datasets" or "datasets:my-ds/events"
    source_count: int
    dest_count: int
    ok: bool
    missing: list[str] = field(default_factory=list)  # object kind only

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "kind": self.kind,
            "scope": self.scope,
            "source_count": self.source_count,
            "dest_count": self.dest_count,
            "ok": self.ok,
        }
        if self.missing:
            out["missing"] = self.missing
        return out


@dataclass
class ResourceValidation:
    resource_type: str
    ok: bool
    checks: list[CheckResult]

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource_type": self.resource_type,
            "ok": self.ok,
            "checks": [c.to_dict() for c in self.checks],
        }


def _default_key(resource: dict[str, Any]) -> str | None:
    """Stable match key: prefer slug (functions/prompts), else name."""
    return resource.get("slug") or resource.get("name")


def _dataset_from_expr(parent_id: str) -> str:
    return f"dataset('{btql_quote(parent_id)}') spans"


def _experiment_from_expr(parent_id: str) -> str:
    return f"experiment('{btql_quote(parent_id)}') spans"


def _logs_from_expr(parent_id: str) -> str:
    return f"project_logs('{btql_quote(parent_id)}') spans"


@dataclass(frozen=True)
class ValidationSpec:
    # Listable resource type for object parity (None for logs, which has no object layer).
    object_type: str | None = None
    # Builds a BTQL `from` expression for the event count, given a parent id.
    event_from_expr: Callable[[str], str] | None = None
    # True: events are counted per listed object; False: project-level (logs).
    event_per_object: bool = False
    key_fn: Callable[[dict[str, Any]], str | None] = _default_key
    # Mirrors migration policy: keep only items the migrator would have migrated.
    expected_filter: Callable[[dict[str, Any]], bool] | None = None


VALIDATION_SPECS: dict[str, ValidationSpec] = {
    "datasets": ValidationSpec(
        object_type="datasets",
        event_from_expr=_dataset_from_expr,
        event_per_object=True,
    ),
    "experiments": ValidationSpec(
        object_type="experiments",
        event_from_expr=_experiment_from_expr,
        event_per_object=True,
    ),
    "logs": ValidationSpec(
        object_type=None,
        event_from_expr=_logs_from_expr,
        event_per_object=False,
    ),
    "functions": ValidationSpec(
        object_type="functions",
        expected_filter=lambda r: not FunctionMigrator._is_code_bundle_function(r),
    ),
    "prompts": ValidationSpec(object_type="prompts"),
    "project_scores": ValidationSpec(object_type="project_scores"),
    "views": ValidationSpec(object_type="views"),
    "project_tags": ValidationSpec(object_type="project_tags"),
    "span_iframes": ValidationSpec(object_type="span_iframes"),
}


async def count_events(
    client: Any,
    from_expr: str,
    *,
    created_after: str | None = None,
    created_before: str | None = None,
) -> int:
    """Cheap event count via a single BTQL aggregation (no row scan)."""
    conditions: list[str] = []
    if isinstance(created_after, str) and created_after:
        conditions.append(f"created >= '{btql_quote(created_after)}'")
    if isinstance(created_before, str) and created_before:
        conditions.append(f"created < '{btql_quote(created_before)}'")
    filter_clause = f"filter: {' and '.join(conditions)}\n" if conditions else ""
    query = f"select: count(1) as n\nfrom: {from_expr}\n{filter_clause}"

    resp = await client.with_retry(
        "validate_count_events",
        lambda: client.raw_request("POST", "/btql", json={"query": query}),
    )
    data = resp.get("data") if isinstance(resp, dict) else None
    if isinstance(data, list) and data and isinstance(data[0], dict):
        row = data[0]
        value = row.get("n")
        if value is None:
            value = next(
                (v for v in row.values() if isinstance(v, (int, float))), 0
            )
        return int(value)
    return 0


async def _validate_object_parity(
    *,
    source_client: Any,
    dest_client: Any,
    spec: ValidationSpec,
    resource_name: str,
    source_project_id: str,
    dest_project_id: str,
    list_fn: ListFn,
) -> tuple[CheckResult, list[dict[str, Any]], list[dict[str, Any]]]:
    assert spec.object_type is not None
    source = await list_fn(source_client, spec.object_type, source_project_id)
    dest = await list_fn(dest_client, spec.object_type, dest_project_id)

    expected = (
        [r for r in source if spec.expected_filter(r)]
        if spec.expected_filter
        else list(source)
    )
    expected_keys = {k for r in expected if (k := spec.key_fn(r))}
    dest_keys = {k for r in dest if (k := spec.key_fn(r))}

    missing = sorted(expected_keys - dest_keys)
    check = CheckResult(
        kind="object",
        scope=resource_name,
        source_count=len(expected_keys),
        dest_count=len(expected_keys & dest_keys),
        ok=not missing,
        missing=missing,
    )
    return check, expected, dest


async def _validate_event_parity(
    *,
    scope: str,
    source_client: Any,
    dest_client: Any,
    source_from_expr: str,
    dest_from_expr: str,
    created_after: str | None,
    created_before: str | None,
) -> CheckResult:
    source_count = await count_events(
        source_client,
        source_from_expr,
        created_after=created_after,
        created_before=created_before,
    )
    dest_count = await count_events(
        dest_client,
        dest_from_expr,
        created_after=created_after,
        created_before=created_before,
    )
    return CheckResult(
        kind="events",
        scope=scope,
        source_count=source_count,
        dest_count=dest_count,
        ok=source_count == dest_count,
    )


async def validate_resource(
    resource_name: str,
    *,
    source_client: Any,
    dest_client: Any,
    source_project_id: str,
    dest_project_id: str,
    list_fn: ListFn,
    id_mapping: dict[str, str] | None = None,
    created_after: str | None = None,
    created_before: str | None = None,
) -> ResourceValidation | None:
    """Validate one resource type for a project. Returns None if not in scope."""
    spec = VALIDATION_SPECS.get(resource_name)
    if spec is None:
        return None

    id_mapping = id_mapping or {}
    checks: list[CheckResult] = []
    expected_objects: list[dict[str, Any]] = []
    dest_objects: list[dict[str, Any]] = []

    if spec.object_type is not None:
        obj_check, expected_objects, dest_objects = await _validate_object_parity(
            source_client=source_client,
            dest_client=dest_client,
            spec=spec,
            resource_name=resource_name,
            source_project_id=source_project_id,
            dest_project_id=dest_project_id,
            list_fn=list_fn,
        )
        checks.append(obj_check)

    if spec.event_from_expr is not None:
        if spec.event_per_object:
            dest_id_by_key = {
                spec.key_fn(r): r.get("id") for r in dest_objects if r.get("id")
            }
            for src in expected_objects:
                source_id = src.get("id")
                key = spec.key_fn(src)
                if not isinstance(source_id, str):
                    continue
                # Prefer the exact migration mapping; fall back to name/slug match.
                dest_id = id_mapping.get(source_id) or dest_id_by_key.get(key)
                if not isinstance(dest_id, str):
                    # The object itself is missing; already reported by the object
                    # check. Don't emit a misleading event check.
                    continue
                checks.append(
                    await _validate_event_parity(
                        scope=f"{resource_name}:{key}/events",
                        source_client=source_client,
                        dest_client=dest_client,
                        source_from_expr=spec.event_from_expr(source_id),
                        dest_from_expr=spec.event_from_expr(dest_id),
                        created_after=created_after,
                        created_before=created_before,
                    )
                )
        else:
            # Project-level events (logs).
            checks.append(
                await _validate_event_parity(
                    scope=f"{resource_name}/events",
                    source_client=source_client,
                    dest_client=dest_client,
                    source_from_expr=spec.event_from_expr(source_project_id),
                    dest_from_expr=spec.event_from_expr(dest_project_id),
                    created_after=created_after,
                    created_before=created_before,
                )
            )

    return ResourceValidation(
        resource_type=resource_name,
        ok=all(c.ok for c in checks),
        checks=checks,
    )
