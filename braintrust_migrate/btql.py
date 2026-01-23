"""BTQL helpers (query execution + resilient paging).

Streaming migrators use BTQL (SQL) queries sorted by `_pagination_key` and need
to be resilient to backend timeouts (504) and internal errors (500) that
correlate with large LIMIT values.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

import httpx
import structlog

from braintrust_migrate.client import BraintrustClient


def btql_quote(s: str) -> str:
    """Escape a string for inclusion in a single-quoted BTQL/SQL literal."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


async def find_first_pagination_key_for_created_after(
    *,
    client: BraintrustClient,
    from_expr: str,
    created_after: str,
    operation: str,
    log_fields: dict[str, Any],
    timeout_seconds: float = 120.0,
) -> str | None:
    """Find the smallest `_pagination_key` for rows where `created >= created_after`.

    This is a one-time preflight used by streaming migrators when users request a
    created-after filter, so we can start pagination near the first matching row.
    """

    def _query_text_for_limit(n: int) -> str:
        return (
            "SELECT _pagination_key\n"
            f"FROM {from_expr}\n"
            f"WHERE created >= '{btql_quote(created_after)}'\n"
            "ORDER BY _pagination_key ASC\n"
            f"LIMIT {int(n)}"
        )

    out = await fetch_btql_sorted_page_with_retries(
        client=client,
        query_for_limit=_query_text_for_limit,
        configured_limit=1,
        operation=operation,
        log_fields=log_fields,
        timeout_seconds=timeout_seconds,
        floor_limit=1,
    )
    lp = out.get("btql_last_pagination_key")
    return lp if isinstance(lp, str) and lp else None


async def fetch_btql_sorted_page_with_retries(
    *,
    client: BraintrustClient,
    query_for_limit: Callable[[int], str],
    configured_limit: int,
    operation: str,
    log_fields: dict[str, Any],
    timeout_seconds: float = 120.0,
    default_500_retry_limit: int = 500,
    floor_limit: int = 25,
) -> dict[str, Any]:
    """Run a BTQL query with retries and return rows + last pagination key.

    Returns a dict shaped like:
      {"events": [...], "cursor": None, "btql_last_pagination_key": "..."}

    Resilience:
    - Retries 500/504 by rerunning the query with smaller LIMIT values (1000, 500, 250, ...).
    """
    logger = structlog.get_logger(__name__)

    HTTP_STATUS_GATEWAY_TIMEOUT = 504
    HTTP_STATUS_INTERNAL_SERVER_ERROR = 500

    async def _do_btql(*, op: str, query_text: str) -> Any:
        return await client.with_retry(
            op,
            lambda: client.raw_request(
                "POST",
                "/btql",
                json={
                    "query": query_text,
                    "fmt": "json",
                    "api_version": 1,
                    # All modern deployments are Brainstore-backed; do not attempt
                    # a Postgres fallback.
                    "use_brainstore": True,
                },
                timeout=timeout_seconds,
            ),
        )

    async def _fetch_one_limit(n: int) -> Any:
        q = query_for_limit(n)
        return await _do_btql(op=operation, query_text=q)

    attempted_limits: list[int] = []
    to_try: list[int] = [int(configured_limit)]
    if int(configured_limit) > default_500_retry_limit:
        to_try.append(default_500_retry_limit)
    last = to_try[-1]
    while last // 2 >= floor_limit:
        last = last // 2
        to_try.append(last)

    # De-dupe while preserving order.
    seen_lims: set[int] = set()
    to_try = [n for n in to_try if not (n in seen_lims or seen_lims.add(n))]

    resp: Any | None = None
    last_err: Exception | None = None
    for n in to_try:
        attempted_limits.append(n)
        try:
            resp = await _fetch_one_limit(n)
            if n != int(configured_limit):
                logger.info(
                    "BTQL succeeded after retrying with smaller LIMIT",
                    configured_fetch_limit=configured_limit,
                    effective_fetch_limit=n,
                    attempted_limits=attempted_limits,
                    **log_fields,
                )
            break
        except httpx.HTTPStatusError as e:
            status = None
            try:
                status = int(e.response.status_code) if e.response is not None else None
            except Exception:
                status = None

            # If the backend times out or returns 500 at higher LIMITs, try again with
            # smaller LIMIT values.
            if (
                status
                in {HTTP_STATUS_INTERNAL_SERVER_ERROR, HTTP_STATUS_GATEWAY_TIMEOUT}
                and n != to_try[-1]
            ):
                logger.warning(
                    "BTQL returned error; retrying with smaller LIMIT",
                    status_code=status,
                    configured_fetch_limit=configured_limit,
                    effective_fetch_limit=n,
                    attempted_limits=attempted_limits,
                    **log_fields,
                )
                last_err = e
                continue
            raise
        except Exception as e:
            last_err = e
            raise

    if resp is None:
        if last_err is not None:
            raise last_err
        raise RuntimeError("BTQL fetch failed without an exception")

    if not isinstance(resp, dict):
        raise TypeError(f"Unexpected btql response type: {type(resp).__name__}")
    rows = resp.get("data")
    if not isinstance(rows, list):
        rows = []

    page_last_pk: str | None = None
    if rows:
        last_row = rows[-1]
        if isinstance(last_row, dict):
            lp = last_row.get("_pagination_key")
            if isinstance(lp, str) and lp:
                page_last_pk = lp

    return {
        "events": cast(list[dict[str, Any]], rows),
        "cursor": None,
        "btql_last_pagination_key": page_last_pk,
    }
