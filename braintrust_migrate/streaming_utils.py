"""Shared utilities for streaming, BTQL-sorted migrations.

This module intentionally contains small, low-level helpers used by multiple
streaming migrators (logs, dataset events, experiment events) to avoid logic
drift across implementations.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypedDict, cast

from braintrust_migrate.batching import (
    approx_events_insert_payload_bytes,
    iter_ordered_batches_by_count_and_bytes,
)
from braintrust_migrate.btql import btql_quote
from braintrust_migrate.insert_bisect import insert_with_413_bisect


class SeenIdsDB:
    """SQLite-backed set of inserted ids (fast idempotency guard across resumes)."""

    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=OFF;")
        self._conn.execute("PRAGMA temp_store=MEMORY;")
        self._conn.execute("CREATE TABLE IF NOT EXISTS seen (id TEXT PRIMARY KEY);")
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def filter_unseen(self, ids: list[str]) -> list[str]:
        if not ids:
            return []
        placeholders = ",".join(["?"] * len(ids))
        cur = self._conn.execute(
            f"SELECT id FROM seen WHERE id IN ({placeholders})", ids
        )
        seen = {row[0] for row in cur.fetchall()}
        return [i for i in ids if i not in seen]

    def mark_seen(self, ids: list[str]) -> None:
        if not ids:
            return
        self._conn.executemany(
            "INSERT OR IGNORE INTO seen (id) VALUES (?)",
            [(i,) for i in ids],
        )
        self._conn.commit()


def build_btql_sorted_page_query(
    *,
    from_expr: str,
    limit: int,
    last_pagination_key: str | None,
    last_pagination_key_inclusive: bool = False,
    created_after: str | None = None,
    select: str = "*",
) -> str:
    """Build a BTQL SQL query for stable sorted paging on `_pagination_key`."""

    conditions: list[str] = []
    if isinstance(created_after, str) and created_after:
        conditions.append(f"created >= '{btql_quote(created_after)}'")
    if isinstance(last_pagination_key, str) and last_pagination_key:
        op = ">=" if last_pagination_key_inclusive else ">"
        conditions.append(f"_pagination_key {op} '{btql_quote(last_pagination_key)}'")
    where = f"WHERE {' AND '.join(conditions)}\n" if conditions else ""

    return (
        f"SELECT {select}\n"
        f"FROM {from_expr}\n"
        f"{where}"
        "ORDER BY _pagination_key ASC\n"
        f"LIMIT {int(limit)}"
    )


class StreamHooks(TypedDict, total=False):
    """Optional hooks to report progress from shared streaming loop."""

    on_fetch: Callable[[dict[str, Any]], None]
    on_insert: Callable[[dict[str, Any]], None]
    on_page: Callable[[dict[str, Any]], None]
    on_done: Callable[[dict[str, Any]], None]
    on_batch_error: Callable[[dict[str, Any]], None]


def _extract_ids(events: list[dict[str, Any]]) -> list[str]:
    ids: list[str] = []
    for e in events:
        i = e.get("id")
        if isinstance(i, str) and i:
            ids.append(i)
    return ids


async def stream_btql_sorted_events(
    *,
    fetch_page: Callable[[int], Awaitable[dict[str, Any]]],
    page_limit: int,
    # State plumbing
    get_last_pk: Callable[[], str | None],
    set_last_pk: Callable[[str | None], None],
    save_state: Callable[[], None],
    # Event processing
    page_event_filter: Callable[[dict[str, Any]], bool] | None,
    event_to_insert: Callable[[dict[str, Any]], dict[str, Any]],
    # Idempotency + batching
    seen_db: SeenIdsDB | None,
    insert_batch_size: int,
    insert_max_bytes: int | None,
    # Optional attachment rewrite (returns count of rewritten refs or 0/1)
    rewrite_event_in_place: Callable[[dict[str, Any]], Awaitable[int]] | None,
    # Insert behavior
    insert_events: Callable[[list[dict[str, Any]]], Awaitable[None]],
    is_http_413: Callable[[Exception], bool],
    on_single_413: Callable[[dict[str, Any], Exception], Awaitable[None]] | None,
    # Counters (mutated via closures so each migrator keeps its own state shape)
    incr_fetched: Callable[[int], None],
    incr_inserted: Callable[[int], None],
    incr_inserted_bytes: Callable[[int], None],
    incr_skipped_deleted: Callable[[int], None] | None,
    incr_skipped_seen: Callable[[int], None] | None,
    incr_attachments_copied: Callable[[int], None] | None,
    # Optional progress hooks
    hooks: StreamHooks | None = None,
) -> None:
    """Shared BTQL-sorted streaming loop.

    This handles:
    - Fetching pages (sorted by `_pagination_key`)
    - Filtering deleted events (optional)
    - Filtering seen IDs via SeenIdsDB (optional)
    - Batching by count and bytes
    - Optional attachment rewrite
    - Insert with 413-bisect isolation
    - Advancing pagination key only after successful inserts
    """

    page_num = 0
    while True:
        page_num += 1
        page = await fetch_page(page_limit)
        page_events: list[dict[str, Any]] = cast(
            list[dict[str, Any]], page.get("events") or []
        )
        page_last_pk: str | None = cast(
            str | None, page.get("btql_last_pagination_key")
        )

        if page_events and hooks and "on_fetch" in hooks:
            hooks["on_fetch"](
                {
                    "page_num": page_num,
                    "page_events": len(page_events),
                    "configured_fetch_limit": int(page_limit),
                }
            )

        if not page_events:
            # Persist end-of-stream state (so resume knows it's complete and counters are saved).
            save_state()
            if hooks and "on_done" in hooks:
                hooks["on_done"]({"page_num": page_num})
            break

        incr_fetched(len(page_events))

        kept: list[dict[str, Any]] = []
        if page_event_filter is None:
            kept = page_events
        else:
            skipped_deleted = 0
            for e in page_events:
                if page_event_filter(e):
                    skipped_deleted += 1
                    continue
                kept.append(e)
            if skipped_deleted and incr_skipped_deleted is not None:
                incr_skipped_deleted(skipped_deleted)

        insert_events_list = [event_to_insert(e) for e in kept]

        # Filter seen ids up-front for the whole page (preserves order and reduces DB calls).
        if seen_db is not None:
            all_ids = _extract_ids(insert_events_list)
            if all_ids:
                unseen = set(seen_db.filter_unseen(all_ids))
                skipped_seen = len(all_ids) - len(unseen)
                if skipped_seen and incr_skipped_seen is not None:
                    incr_skipped_seen(skipped_seen)
                insert_events_list = [
                    e for e in insert_events_list if e.get("id") in unseen
                ]

        async def _insert_one(batch: list[dict[str, Any]]) -> float:
            t0 = time.perf_counter()
            await insert_events(batch)
            return max(0.0, time.perf_counter() - t0)

        async def _on_success(batch: list[dict[str, Any]], dt: float) -> None:
            if seen_db is not None:
                seen_db.mark_seen(_extract_ids(batch))
            incr_inserted(len(batch))
            inserted_bytes_last = approx_events_insert_payload_bytes(batch)
            incr_inserted_bytes(inserted_bytes_last)
            if hooks and "on_insert" in hooks:
                hooks["on_insert"](
                    {
                        "inserted_last": len(batch),
                        "inserted_bytes_last": inserted_bytes_last,
                        "insert_seconds": dt,
                    }
                )

        async def _on_single_413(event: dict[str, Any], err: Exception) -> None:
            if on_single_413 is not None:
                await on_single_413(event, err)

        for batch in iter_ordered_batches_by_count_and_bytes(
            insert_events_list,
            max_items=int(insert_batch_size),
            max_bytes=insert_max_bytes,
        ):
            if not batch:
                continue

            if (
                rewrite_event_in_place is not None
                and incr_attachments_copied is not None
            ):
                copied = 0
                for e in batch:
                    copied += int(await rewrite_event_in_place(e))
                if copied:
                    incr_attachments_copied(copied)

            try:
                await insert_with_413_bisect(
                    batch,
                    insert_fn=_insert_one,
                    is_http_413=is_http_413,
                    on_success=_on_success,
                    on_single_413=_on_single_413,
                )
            except Exception as e:
                if hooks and "on_batch_error" in hooks:
                    hooks["on_batch_error"](
                        {
                            "page_num": page_num,
                            "batch": batch,
                            "error": e,
                        }
                    )
                raise

        # Commit pagination progress only after inserts for this page succeed.
        if isinstance(page_last_pk, str) and page_last_pk:
            set_last_pk(page_last_pk)
        save_state()

        if hooks and "on_page" in hooks:
            hooks["on_page"](
                {
                    "page_num": page_num,
                    "page_events": len(page_events),
                }
            )
