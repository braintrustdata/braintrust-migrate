from __future__ import annotations

from braintrust_migrate.batching import (
    approx_json_bytes,
    iter_ordered_batches_by_count_and_bytes,
)


def test_byte_batcher_splits_by_bytes_and_preserves_order() -> None:
    # Two items that individually fit under the cap, but together exceed it.
    items = [
        {"id": "a", "input": "x" * 200},
        {"id": "b", "input": "y" * 200},
        {"id": "c", "input": "z" * 200},
    ]

    one_item_bytes = approx_json_bytes({"events": [items[0]]})
    two_item_bytes = approx_json_bytes({"events": [items[0], items[1]]})
    assert one_item_bytes < two_item_bytes

    max_bytes = one_item_bytes + 10

    batches = list(
        iter_ordered_batches_by_count_and_bytes(
            items,
            max_items=1000,
            max_bytes=max_bytes,
            exact_wrapper_bytes=True,
        )
    )

    assert [x["id"] for b in batches for x in b] == ["a", "b", "c"]
    assert len(batches) >= 2
    for b in batches:
        assert approx_json_bytes({"events": b}) <= max_bytes
