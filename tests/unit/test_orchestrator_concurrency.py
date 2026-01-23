import asyncio

import pytest

from braintrust_migrate.orchestration import _gather_with_concurrency


@pytest.mark.asyncio
async def test_gather_with_concurrency_respects_limit() -> None:
    max_concurrent = 3

    lock = asyncio.Lock()
    start = asyncio.Event()
    in_flight = 0
    max_seen = 0

    async def job(i: int) -> int:
        nonlocal in_flight, max_seen
        await start.wait()
        async with lock:
            in_flight += 1
            max_seen = max(max_seen, in_flight)
        await asyncio.sleep(0.01)
        async with lock:
            in_flight -= 1
        return i

    coros = [job(i) for i in range(10)]

    gather_task = asyncio.create_task(
        _gather_with_concurrency(coros, max_concurrent=max_concurrent)
    )
    await asyncio.sleep(0)  # allow tasks to start and block on `start`
    start.set()

    results = await gather_task
    values = [r for r in results if not isinstance(r, Exception)]

    assert sorted(values) == list(range(10))
    assert max_seen <= max_concurrent


@pytest.mark.asyncio
async def test_gather_with_concurrency_captures_exceptions() -> None:
    max_concurrent = 2
    start = asyncio.Event()

    async def ok(i: int) -> int:
        await start.wait()
        await asyncio.sleep(0.01)
        return i

    async def boom() -> int:
        await start.wait()
        await asyncio.sleep(0.005)
        raise RuntimeError("nope")

    coros = [ok(1), boom(), ok(2)]
    task = asyncio.create_task(
        _gather_with_concurrency(coros, max_concurrent=max_concurrent)
    )
    await asyncio.sleep(0)
    start.set()

    results = await task
    assert results[0] == 1
    assert isinstance(results[1], RuntimeError)
    assert results[2] == 2
