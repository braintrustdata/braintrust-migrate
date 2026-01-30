"""Insert resilience helpers.

Streaming migrators (logs / experiment events / dataset events) need to handle
HTTP 413 payload limits robustly. We do this by bisecting batches (preserving
order) until the payload is accepted, and isolating single events that are too
large to ever insert.
"""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")
R = TypeVar("R")


async def insert_with_413_bisect(
    items: list[T],
    *,
    insert_fn: Callable[[list[T]], Awaitable[R]],
    is_http_413: Callable[[Exception], bool],
    on_success: Callable[[list[T], R], Awaitable[None]] | None = None,
    on_single_413: Callable[[T, Exception], Awaitable[None]] | None = None,
    skip_single_413: bool = False,
) -> list[T]:
    """Insert items, bisecting on 413 to isolate oversized payloads.

    - Preserves the original item order by always processing the left half first.
    - If a singleton batch triggers 413, invokes `on_single_413` (if provided).
    - If `skip_single_413` is True, the oversized item is skipped and migration
      continues. If False (default), the exception is re-raised.

    Returns:
        List of items that were skipped due to being oversized (empty if none).
    """
    skipped_oversize: list[T] = []
    stack: list[list[T]] = [items]
    while stack:
        batch = stack.pop()
        if not batch:
            continue
        try:
            res = await insert_fn(batch)
            if on_success is not None:
                await on_success(batch, res)
        except Exception as e:
            if is_http_413(e):
                if len(batch) == 1:
                    if on_single_413 is not None:
                        await on_single_413(batch[0], e)
                    if skip_single_413:
                        skipped_oversize.append(batch[0])
                        continue
                    raise
                mid = math.ceil(len(batch) / 2)
                left = batch[:mid]
                right = batch[mid:]
                # Process left first for determinism.
                stack.append(right)
                stack.append(left)
                continue
            raise
    return skipped_oversize