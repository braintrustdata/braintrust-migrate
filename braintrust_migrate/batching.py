"""Helpers for batching inserts by both count and approximate payload size."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from typing import Any


# We deliberately use the *non-compact* json encoding here to slightly
# over-estimate payload sizes vs compact separators. This works well with a
# headroom ratio to avoid flirting with gateway limits.
def approx_json_bytes(obj: Any) -> int:
    return len(json.dumps(obj, ensure_ascii=False))


_EMPTY_EVENTS_WRAPPER_BYTES = approx_json_bytes({"events": []})


def approx_events_insert_payload_bytes(
    events: list[dict[str, Any]],
    *,
    approx_event_bytes: Callable[[dict[str, Any]], int] | None = None,
) -> int:
    """Approximate the JSON payload bytes for `{"events": events}`.

    This uses a fast additive estimate: wrapper overhead + sum(event bytes) + commas.
    It intentionally overestimates in common cases.
    """
    if not events:
        return _EMPTY_EVENTS_WRAPPER_BYTES

    if approx_event_bytes is None:

        def _default_approx_event_bytes(e: dict[str, Any]) -> int:
            return approx_json_bytes(e)

        approx_event_bytes = _default_approx_event_bytes

    total = _EMPTY_EVENTS_WRAPPER_BYTES
    # When the array is non-empty, we need to account for commas between items.
    # (JSON list uses n-1 commas.)
    total += max(0, len(events) - 1)
    for e in events:
        total += int(approx_event_bytes(e))
    return total


def iter_ordered_batches_by_count_and_bytes(
    items: list[dict[str, Any]],
    *,
    max_items: int,
    max_bytes: int | None,
    approx_item_bytes: Callable[[dict[str, Any]], int] | None = None,
    # If True, compute payload bytes for wrapper {"events": batch} rather than just
    # item-by-item (slower, but more accurate). Defaults to False because we also
    # apply headroom and keep a 413-bisect fallback.
    exact_wrapper_bytes: bool = False,
) -> Iterable[list[dict[str, Any]]]:
    """Yield ordered batches constrained by max items and (approx) bytes.

    Preserves item order. If a single item exceeds `max_bytes`, it is yielded as a
    singleton batch (and may still fail server-side; callers should handle that).
    """
    if max_items <= 0:
        raise ValueError(f"max_items must be positive; got {max_items}")
    if max_bytes is not None and max_bytes <= 0:
        raise ValueError(f"max_bytes must be positive; got {max_bytes}")

    if not items:
        return

    if approx_item_bytes is None:

        def _default_approx_item_bytes(e: dict[str, Any]) -> int:
            return approx_json_bytes(e)

        approx_item_bytes = _default_approx_item_bytes

    batch: list[dict[str, Any]] = []

    def _payload_bytes(b: list[dict[str, Any]]) -> int:
        if max_bytes is None:
            return 0
        if exact_wrapper_bytes:
            return approx_json_bytes({"events": b})
        return approx_events_insert_payload_bytes(
            b, approx_event_bytes=approx_item_bytes
        )

    for item in items:
        if not batch:
            batch = [item]
            continue

        would_exceed_items = len(batch) >= max_items
        if would_exceed_items:
            yield batch
            batch = [item]
            continue

        if max_bytes is None:
            batch.append(item)
            continue

        # Try to add the item and check the payload bytes.
        candidate = [*batch, item]
        candidate_bytes = _payload_bytes(candidate)
        if candidate_bytes <= max_bytes:
            batch = candidate
            continue

        # Flush current batch; start new batch with this item (even if it exceeds).
        yield batch
        batch = [item]

    if batch:
        yield batch
