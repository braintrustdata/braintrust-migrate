"""Shared utilities for streaming, BTQL-sorted migrations.

This module intentionally contains small, low-level helpers used by multiple
streaming migrators (logs, dataset events, experiment events) to avoid logic
drift across implementations.
"""

from __future__ import annotations

import json as _json
import sqlite3
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypedDict, cast

import httpx

from braintrust_migrate.batching import (
    approx_events_insert_payload_bytes,
    approx_json_bytes,
)
from braintrust_migrate.btql import btql_quote

# HTTP status codes
HTTP_STATUS_REQUEST_ENTITY_TOO_LARGE = 413


def is_http_413(exc: Exception) -> bool:
    """Whether ``exc`` is an HTTP 413 (Request Entity Too Large) error."""
    return (
        isinstance(exc, httpx.HTTPStatusError)
        and exc.response is not None
        and int(exc.response.status_code) == HTTP_STATUS_REQUEST_ENTITY_TOO_LARGE
    )


def approx_event_size_bytes(event: dict[str, Any]) -> int | None:
    """Compact serialized size of an event, or None if it can't be serialized."""
    try:
        return len(_json.dumps(event, separators=(",", ":"), ensure_ascii=False))
    except Exception:
        return None


def count_attachment_refs(event: dict[str, Any]) -> int:
    """Count braintrust_attachment references anywhere within an event."""

    def _walk(v: Any) -> int:
        if isinstance(v, dict):
            if v.get("type") == "braintrust_attachment" and isinstance(
                v.get("key"), str
            ):
                return 1
            return sum(_walk(x) for x in v.values())
        if isinstance(v, list):
            return sum(_walk(x) for x in v)
        return 0

    return _walk(event)


def dump_oversize_event_summary(
    *,
    out_dir: Path,
    filename_prefix: str,
    event_label: str,
    dest_id_field: str,
    dest_id_value: str | None,
    cursor: str | None,
    event: dict[str, Any],
    error: Exception,
    logger: Any,
) -> None:
    """Write a diagnostic summary for a single event that cannot be inserted (413).

    Pure side effect: writes a JSON file under ``out_dir`` and logs. Never touches
    stream state, the seen-ids DB, or the insert flow, and never raises (a failed
    write is logged and swallowed so it cannot abort the streaming loop).
    """
    event_id = event.get("id")
    safe_id = str(event_id) if isinstance(event_id, str) and event_id else "unknown"
    root_span_id = event.get("root_span_id")
    span_id = event.get("span_id")
    approx_size = approx_event_size_bytes(event)
    attachment_refs = count_attachment_refs(event)
    path = out_dir / f"{filename_prefix}{safe_id}.json"
    summary = {
        "error": str(error),
        "cursor": cursor,
        dest_id_field: dest_id_value,
        "event_id": event.get("id"),
        "root_span_id": root_span_id,
        "span_id": span_id,
        "created": event.get("created"),
        "approx_size_bytes": approx_size,
        "attachment_refs": attachment_refs,
        "top_level_keys": sorted(list(event.keys())),
    }
    try:
        with open(path, "w") as f:
            _json.dump(summary, f, indent=2)
        logger.error(
            f"Oversize {event_label} isolated (413). This specific event cannot be inserted.",
            summary_path=str(path),
            event_id=safe_id,
            root_span_id=root_span_id,
            span_id=span_id,
            approx_size_bytes=approx_size,
            attachment_refs=attachment_refs,
            cursor=cursor,
        )
    except Exception:
        logger.error(
            f"Oversize {event_label} isolated; failed to write summary",
            event_id=safe_id,
            root_span_id=root_span_id,
            span_id=span_id,
            cursor=cursor,
        )


@dataclass
class EventsStreamState:
    """Checkpoint state for streaming events migration (datasets/experiments)."""

    version: str | None = None
    cursor: str | None = None
    btql_min_pagination_key: str | None = None
    fetched_events: int = 0
    inserted_events: int = 0
    inserted_bytes: int = 0
    skipped_deleted: int = 0
    skipped_seen: int = 0
    attachments_copied: int = 0
    spilled_fields: int = 0

    @classmethod
    def from_path(cls, path: Path) -> EventsStreamState:
        if not path.exists():
            return cls()
        with open(path) as f:
            data = _json.load(f)
        return cls(
            version=data.get("version"),
            cursor=data.get("cursor"),
            btql_min_pagination_key=data.get("btql_min_pagination_key"),
            fetched_events=int(data.get("fetched_events", 0)),
            inserted_events=int(data.get("inserted_events", 0)),
            inserted_bytes=int(data.get("inserted_bytes", 0)),
            skipped_deleted=int(data.get("skipped_deleted", 0)),
            skipped_seen=int(data.get("skipped_seen", 0)),
            attachments_copied=int(data.get("attachments_copied", 0)),
            spilled_fields=int(data.get("spilled_fields", 0)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "cursor": self.cursor,
            "btql_min_pagination_key": self.btql_min_pagination_key,
            "fetched_events": self.fetched_events,
            "inserted_events": self.inserted_events,
            "inserted_bytes": self.inserted_bytes,
            "skipped_deleted": self.skipped_deleted,
            "skipped_seen": self.skipped_seen,
            "attachments_copied": self.attachments_copied,
            "spilled_fields": self.spilled_fields,
        }


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


def coerce_int_config(
    cfg: Any,
    attr_name: str,
    default: int,
    *,
    minimum: int | None = None,
) -> int:
    """Best-effort int coercion for migration config values.

    This is intentionally tolerant of mocks and unset attributes because many
    unit tests construct lightweight client doubles without real config models.
    """

    value = getattr(cfg, attr_name, default)
    if not isinstance(value, int):
        try:
            value = int(value)
        except Exception:
            value = default
    if minimum is not None and value < minimum:
        return default
    return value


# Default streaming flush/fetch knobs. Single source of truth: the migrators
# expose these as ClassVars for back-compat and tests.
STREAMING_FLUSH_MAX_ROWS = 5_000
STREAMING_FLUSH_MAX_BYTES = 25 * 1024 * 1024
STREAMING_MAX_EVENT_BYTES = 18 * 1024 * 1024
STREAMING_EVENT_FETCH_GROUP_SIZE = 25


@dataclass(frozen=True)
class StreamingConfig:
    """Resolved streaming flush/fetch configuration for the event migrators.

    Replaces the byte-batching config block that was duplicated (with a drifting
    headroom default) across the logs, dataset, and experiment migrators.
    """

    sdk_flush_max_rows: int
    sdk_flush_max_bytes: int
    max_event_bytes: int
    event_fetch_group_size: int

    @classmethod
    def resolve(cls, source_client: Any, dest_client: Any) -> StreamingConfig:
        """Resolve config from the dest client's migration_config (or source's).

        ``events_flush_max_rows`` / ``events_fetch_group_size`` are the only
        env-overridable knobs; the byte caps are fixed. Falls back to the module
        defaults and is tolerant of lightweight test doubles / missing config.
        """
        cfg = getattr(dest_client, "migration_config", None) or getattr(
            source_client, "migration_config", None
        )
        return cls(
            sdk_flush_max_rows=coerce_int_config(
                cfg, "events_flush_max_rows", STREAMING_FLUSH_MAX_ROWS, minimum=1
            ),
            sdk_flush_max_bytes=STREAMING_FLUSH_MAX_BYTES,
            max_event_bytes=STREAMING_MAX_EVENT_BYTES,
            event_fetch_group_size=coerce_int_config(
                cfg,
                "events_fetch_group_size",
                STREAMING_EVENT_FETCH_GROUP_SIZE,
                minimum=1,
            ),
        )


def _truncate_cursor(state: Any) -> str | None:
    """Short, display-friendly form of the current pagination key."""
    pk = state.btql_min_pagination_key
    return (pk[:16] + "…") if isinstance(pk, str) else None


def build_stream_progress(
    phase: str,
    info: dict[str, Any],
    state: Any,
    *,
    resource: str,
    id_fields: dict[str, Any],
) -> dict[str, Any]:
    """Build a normalized streaming progress payload for the dataset/experiment
    event migrators.

    Replaces the four near-identical per-migrator hook lambdas. ``resource`` and
    ``id_fields`` carry the per-resource bits (e.g. ``"dataset_events"`` and
    ``{"source_dataset_ids": [...], "dest_dataset_ids": [...]}``); everything else
    comes from the loop's ``info`` payload or ``state`` exactly as before.
    """
    payload: dict[str, Any] = {"resource": resource, "phase": phase, **id_fields}

    if phase in ("fetch", "page"):
        payload.update(
            {
                "page_num": info.get("page_num"),
                "page_events": info.get("page_events"),
                "fetched_total": info.get("fetched_total"),
                "inserted_total": info.get("inserted_total"),
                "inserted_bytes_total": info.get("inserted_bytes_total"),
                "skipped_deleted_total": info.get("skipped_deleted_total"),
                "skipped_seen_total": info.get("skipped_seen_total"),
                "attachments_copied_total": info.get("attachments_copied_total"),
                "pending_buffered_rows": info.get("pending_buffered_rows"),
                "pending_buffered_bytes": info.get("pending_buffered_bytes"),
                "cursor": _truncate_cursor(state),
                "next_cursor": None,
            }
        )
    elif phase == "insert":
        # Totals come from committed state (not the per-batch info); page fields
        # are not meaningful for an insert event.
        payload.update(
            {
                "page_num": None,
                "page_events": None,
                "inserted_last": info.get("inserted_last"),
                "inserted_bytes_last": info.get("inserted_bytes_last"),
                "insert_seconds": info.get("insert_seconds"),
                "flush_rows": info.get("flush_rows"),
                "flush_buffer_bytes": info.get("flush_buffer_bytes"),
                "fetched_total": state.fetched_events,
                "inserted_total": state.inserted_events,
                "inserted_bytes_total": state.inserted_bytes,
                "skipped_deleted_total": state.skipped_deleted,
                "skipped_seen_total": state.skipped_seen,
                "attachments_copied_total": state.attachments_copied,
                "pending_buffered_rows": 0,
                "pending_buffered_bytes": 0,
                "cursor": _truncate_cursor(state),
                "next_cursor": None,
            }
        )
    elif phase == "done":
        payload.update(
            {
                "fetched_total": info.get("fetched_total"),
                "inserted_total": info.get("inserted_total"),
                "inserted_bytes_total": info.get("inserted_bytes_total"),
                "skipped_deleted_total": info.get("skipped_deleted_total"),
                "skipped_seen_total": info.get("skipped_seen_total"),
                "attachments_copied_total": info.get("attachments_copied_total"),
                "pending_buffered_rows": info.get("pending_buffered_rows"),
                "pending_buffered_bytes": info.get("pending_buffered_bytes"),
                "cursor": None,
                "next_cursor": None,
            }
        )

    return payload


def make_stream_progress_hooks(
    progress: Callable[[dict[str, Any]], None] | None,
    state: Any,
    *,
    resource: str,
    id_fields: dict[str, Any],
) -> StreamHooks | None:
    """Build the four streaming hooks that emit normalized progress payloads.

    Returns ``None`` when no progress callback is provided, matching the
    ``hooks=None`` shape ``stream_btql_sorted_events_buffered`` expects.
    """
    if progress is None:
        return None

    def _hook(phase: str) -> Callable[[dict[str, Any]], None]:
        return lambda info, _p=progress: _p(
            build_stream_progress(
                phase, info, state, resource=resource, id_fields=id_fields
            )
        )

    return {
        "on_fetch": _hook("fetch"),
        "on_page": _hook("page"),
        "on_insert": _hook("insert"),
        "on_done": _hook("done"),
    }


def build_btql_sorted_page_query(
    *,
    from_expr: str,
    limit: int,
    last_pagination_key: str | None,
    last_pagination_key_inclusive: bool = False,
    created_after: str | None = None,
    created_before: str | None = None,
    select: str = "*",
) -> str:
    """Build a native BTQL query for stable sorted paging on `_pagination_key`.

    Uses native BTQL syntax (select:/from:/filter:/sort:/limit:) instead of SQL
    for compatibility with data planes that don't yet support SQL mode.

    Args:
        from_expr: The FROM expression (e.g., "project_logs('...') spans")
        limit: Maximum number of rows to return
        last_pagination_key: Resume pagination from this key
        last_pagination_key_inclusive: If True, use >= instead of > for pagination key
        created_after: Only include rows with created >= this value (inclusive)
        created_before: Only include rows with created < this value (exclusive)
        select: Fields to select (default "*")

    Returns:
        Native BTQL query string
    """
    conditions: list[str] = []
    if isinstance(created_after, str) and created_after:
        conditions.append(f"created >= '{btql_quote(created_after)}'")
    if isinstance(created_before, str) and created_before:
        conditions.append(f"created < '{btql_quote(created_before)}'")
    if isinstance(last_pagination_key, str) and last_pagination_key:
        op = ">=" if last_pagination_key_inclusive else ">"
        conditions.append(f"_pagination_key {op} '{btql_quote(last_pagination_key)}'")
    filter_clause = f"filter: {' and '.join(conditions)}\n" if conditions else ""

    return (
        f"select: {select}\n"
        f"from: {from_expr}\n"
        f"{filter_clause}"
        "sort: _pagination_key asc\n"
        f"limit: {int(limit)}"
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


async def stream_btql_sorted_events_buffered(
    *,
    fetch_page: Callable[[int], Awaitable[dict[str, Any]]],
    page_limit: int,
    state: EventsStreamState,
    save_state: Callable[[], None],
    page_event_filter: Callable[[dict[str, Any]], bool] | None,
    event_to_insert: Callable[[dict[str, Any]], dict[str, Any]],
    seen_db: SeenIdsDB | None,
    rewrite_event_in_place: Callable[[dict[str, Any]], Awaitable[int]] | None,
    spill_event_in_place: Callable[[dict[str, Any], int], Awaitable[tuple[int, int]]]
    | None = None,
    insert_events: Callable[[list[dict[str, Any]]], Awaitable[None]],
    flush_max_rows: int,
    flush_max_bytes: int,
    is_http_413: Callable[[Exception], bool],
    on_single_413: Callable[[dict[str, Any], Exception], Awaitable[None]] | None,
    hooks: StreamHooks | None = None,
) -> None:
    """Stream BTQL-sorted rows, buffering multiple pages before flushing.

    This preserves restart safety by checkpointing only after a buffered flush
    succeeds. Rows may therefore be fetched across several pages before any are
    committed to the destination or to the seen-id database.
    """

    if flush_max_rows <= 0:
        raise ValueError(f"flush_max_rows must be positive; got {flush_max_rows}")
    if flush_max_bytes <= 0:
        raise ValueError(f"flush_max_bytes must be positive; got {flush_max_bytes}")

    active_last_pk = state.btql_min_pagination_key
    pending_events: list[dict[str, Any]] = []
    pending_seen_ids: set[str] = set()
    pending_row_bytes = 0
    pending_fetched_events = 0
    pending_inserted_events = 0
    pending_inserted_bytes = 0
    pending_skipped_deleted = 0
    pending_skipped_seen = 0
    pending_attachments_copied = 0
    pending_spilled_fields = 0
    pending_last_pk: str | None = None
    current_page_num: int | None = None

    async def _flush_pending() -> None:
        nonlocal active_last_pk
        nonlocal pending_events
        nonlocal pending_seen_ids
        nonlocal pending_row_bytes
        nonlocal pending_fetched_events
        nonlocal pending_inserted_events
        nonlocal pending_inserted_bytes
        nonlocal pending_skipped_deleted
        nonlocal pending_skipped_seen
        nonlocal pending_attachments_copied
        nonlocal pending_spilled_fields
        nonlocal pending_last_pk

        if (
            pending_fetched_events == 0
            and pending_inserted_events == 0
            and pending_skipped_deleted == 0
            and pending_skipped_seen == 0
            and pending_attachments_copied == 0
            and pending_spilled_fields == 0
            and pending_last_pk is None
        ):
            return

        if pending_events:
            batch = list(pending_events)
            started = time.perf_counter()
            try:
                await insert_events(batch)
            except Exception as e:
                if hooks and "on_batch_error" in hooks:
                    hooks["on_batch_error"](
                        {
                            "page_num": current_page_num,
                            "batch": batch,
                            "error": e,
                        }
                    )
                if len(batch) == 1 and on_single_413 is not None and is_http_413(e):
                    await on_single_413(batch[0], e)
                raise

            if seen_db is not None and pending_seen_ids:
                seen_db.mark_seen(list(pending_seen_ids))
            state.inserted_events += pending_inserted_events
            state.inserted_bytes += pending_inserted_bytes
            if hooks and "on_insert" in hooks:
                hooks["on_insert"](
                    {
                        "inserted_last": pending_inserted_events,
                        "inserted_bytes_last": pending_inserted_bytes,
                        "insert_seconds": max(0.0, time.perf_counter() - started),
                        "flush_rows": pending_inserted_events,
                        "flush_buffer_bytes": pending_row_bytes,
                    }
                )

        state.fetched_events += pending_fetched_events
        state.skipped_deleted += pending_skipped_deleted
        state.skipped_seen += pending_skipped_seen
        state.attachments_copied += pending_attachments_copied
        state.spilled_fields += pending_spilled_fields
        state.btql_min_pagination_key = pending_last_pk
        save_state()
        active_last_pk = pending_last_pk

        pending_events = []
        pending_seen_ids = set()
        pending_row_bytes = 0
        pending_fetched_events = 0
        pending_inserted_events = 0
        pending_inserted_bytes = 0
        pending_skipped_deleted = 0
        pending_skipped_seen = 0
        pending_attachments_copied = 0
        pending_spilled_fields = 0
        pending_last_pk = None

    page_num = 0
    while True:
        page_num += 1
        state.btql_min_pagination_key = active_last_pk
        current_page_num = page_num
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
                    "fetched_total": state.fetched_events + pending_fetched_events,
                    "inserted_total": state.inserted_events,
                    "inserted_bytes_total": state.inserted_bytes,
                    "skipped_deleted_total": state.skipped_deleted
                    + pending_skipped_deleted,
                    "skipped_seen_total": state.skipped_seen + pending_skipped_seen,
                    "attachments_copied_total": state.attachments_copied
                    + pending_attachments_copied,
                    "pending_buffered_rows": pending_inserted_events,
                    "pending_buffered_bytes": pending_row_bytes,
                }
            )

        if not page_events:
            await _flush_pending()
            save_state()
            if hooks and "on_done" in hooks:
                hooks["on_done"](
                    {
                        "page_num": page_num,
                        "fetched_total": state.fetched_events,
                        "inserted_total": state.inserted_events,
                        "inserted_bytes_total": state.inserted_bytes,
                        "skipped_deleted_total": state.skipped_deleted,
                        "skipped_seen_total": state.skipped_seen,
                        "attachments_copied_total": state.attachments_copied,
                        "pending_buffered_rows": 0,
                        "pending_buffered_bytes": 0,
                    }
                )
            break

        pending_fetched_events += len(page_events)

        kept: list[dict[str, Any]] = []
        if page_event_filter is None:
            kept = page_events
        else:
            skipped_deleted = 0
            for event in page_events:
                if page_event_filter(event):
                    skipped_deleted += 1
                    continue
                kept.append(event)
            pending_skipped_deleted += skipped_deleted

        insert_events_list = [event_to_insert(event) for event in kept]

        if seen_db is not None:
            all_ids = _extract_ids(insert_events_list)
            if all_ids:
                unseen = set(seen_db.filter_unseen(all_ids))
                pending_skipped_seen += len(all_ids) - len(unseen)
                insert_events_list = [
                    event for event in insert_events_list if event.get("id") in unseen
                ]

        if pending_seen_ids:
            deduped_events: list[dict[str, Any]] = []
            pending_duplicates = 0
            for event in insert_events_list:
                event_id = event.get("id")
                if isinstance(event_id, str) and event_id in pending_seen_ids:
                    pending_duplicates += 1
                    continue
                deduped_events.append(event)
            pending_skipped_seen += pending_duplicates
            insert_events_list = deduped_events

        if insert_events_list:
            # Single pass per event: copy source attachments (if enabled), measure
            # size once, spill oversized rows into attachments, then reuse that
            # size for byte accounting (avoids re-serializing each event).
            copied = 0
            spilled = 0
            event_sizes: list[int] = []
            for event in insert_events_list:
                if rewrite_event_in_place is not None:
                    copied += int(await rewrite_event_in_place(event))
                size = approx_json_bytes(event)
                if spill_event_in_place is not None:
                    spilled_count, size = await spill_event_in_place(event, size)
                    spilled += spilled_count
                event_sizes.append(size)

            pending_attachments_copied += copied
            pending_spilled_fields += spilled
            pending_events.extend(insert_events_list)
            pending_seen_ids.update(_extract_ids(insert_events_list))
            pending_inserted_events += len(insert_events_list)
            size_iter = iter(event_sizes)
            pending_inserted_bytes += approx_events_insert_payload_bytes(
                insert_events_list,
                approx_event_bytes=lambda _e: next(size_iter),
            )
            pending_row_bytes += sum(event_sizes)

        pending_last_pk = page_last_pk
        if isinstance(page_last_pk, str) and page_last_pk:
            active_last_pk = page_last_pk

        if (
            pending_inserted_events >= flush_max_rows
            or pending_row_bytes >= flush_max_bytes
        ):
            await _flush_pending()

        if hooks and "on_page" in hooks:
            hooks["on_page"](
                {
                    "page_num": page_num,
                    "page_events": len(page_events),
                    "fetched_total": state.fetched_events + pending_fetched_events,
                    "inserted_total": state.inserted_events,
                    "inserted_bytes_total": state.inserted_bytes,
                    "skipped_deleted_total": state.skipped_deleted
                    + pending_skipped_deleted,
                    "skipped_seen_total": state.skipped_seen + pending_skipped_seen,
                    "attachments_copied_total": state.attachments_copied
                    + pending_attachments_copied,
                    "pending_buffered_rows": pending_inserted_events,
                    "pending_buffered_bytes": pending_row_bytes,
                }
            )
