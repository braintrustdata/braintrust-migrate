"""Detect source environments, which the migration tool does not migrate.

Environments (e.g. "production", "staging") are org-level, and prompt/function
versions can be assigned to them. Applications that load prompts with
``environment=...`` will fail against the destination until the environments
and their assignments are recreated, so we warn up front rather than let the
migration report look clean.
"""

from typing import Any

import structlog

from braintrust_migrate.client import BraintrustClient

logger = structlog.get_logger(__name__)


async def check_unmigrated_environments(
    source_client: BraintrustClient,
) -> dict[str, Any] | None:
    """Return a warning entry if the source org has environments, else None.

    Never raises: a failed check is logged and treated as "nothing to report",
    since this is advisory and must not block the migration.
    """
    try:
        resp = await source_client.with_retry(
            "list_source_environments",
            lambda: source_client.raw_request("GET", "/environment"),
        )
        objects = resp.get("objects") if isinstance(resp, dict) else None
        if not isinstance(objects, list):
            raise ValueError(f"Unexpected environment list response: {resp!r}")
    except Exception as e:
        logger.warning(
            "Could not check source environments; environments are not migrated",
            error=str(e),
        )
        return None

    slugs = sorted(
        env["slug"]
        for env in objects
        if isinstance(env, dict)
        and isinstance(env.get("slug"), str)
        and not env.get("deleted_at")
    )
    if not slugs:
        return None

    message = (
        f"Source org has {len(slugs)} environment(s) ({', '.join(slugs)}). "
        "Environments and their prompt/function version assignments are NOT "
        "migrated, and only the latest version of each prompt/function is "
        "copied. Recreate the environments and reassign versions in the "
        "destination before pointing apps that load prompts by environment "
        "at it."
    )
    logger.warning(message, environments=slugs)
    return {
        "type": "unmigrated_environments",
        "environments": slugs,
        "message": message,
    }
