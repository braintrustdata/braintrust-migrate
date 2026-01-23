from typing import cast

import httpx
import pytest
from pydantic import HttpUrl

from braintrust_migrate.client import BraintrustClient
from braintrust_migrate.config import BraintrustOrgConfig, MigrationConfig


@pytest.mark.asyncio
async def test_with_retry_respects_retry_after_on_429(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    API_URL = "https://api.braintrust.dev"
    RETRY_DELAY = 0.1
    RETRY_AFTER_SECONDS = 0.25
    EXPECTED_ATTEMPTS = 2

    cfg = MigrationConfig(retry_attempts=EXPECTED_ATTEMPTS - 1, retry_delay=RETRY_DELAY)
    client = BraintrustClient(
        BraintrustOrgConfig(api_key="x", url=cast(HttpUrl, HttpUrl(API_URL))),
        cfg,
        "test",
    )

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("braintrust_migrate.client.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("braintrust_migrate.client.random.uniform", lambda a, b: 0.0)

    calls = {"n": 0}

    async def op() -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            req = httpx.Request("POST", f"{API_URL}/v1/foo")
            resp = httpx.Response(
                429, headers={"Retry-After": str(RETRY_AFTER_SECONDS)}, request=req
            )
            raise httpx.HTTPStatusError("rate limited", request=req, response=resp)
        return "ok"

    result = await client.with_retry("rate_limited_op", lambda: op())
    assert result == "ok"
    assert calls["n"] == EXPECTED_ATTEMPTS
    assert len(sleeps) == 1
    assert sleeps[0] >= RETRY_AFTER_SECONDS


@pytest.mark.asyncio
async def test_with_retry_does_not_retry_on_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    API_URL = "https://api.braintrust.dev"
    RETRY_DELAY = 0.1

    cfg = MigrationConfig(retry_attempts=3, retry_delay=RETRY_DELAY)
    client = BraintrustClient(
        BraintrustOrgConfig(api_key="x", url=cast(HttpUrl, HttpUrl(API_URL))),
        cfg,
        "test",
    )

    # Ensure we don't actually sleep in case of a regression
    async def boom_sleep(_: float) -> None:
        raise AssertionError("should not sleep for non-retryable errors")

    monkeypatch.setattr("braintrust_migrate.client.asyncio.sleep", boom_sleep)

    async def op() -> None:
        req = httpx.Request("GET", f"{API_URL}/v1/foo")
        resp = httpx.Response(400, request=req)
        raise httpx.HTTPStatusError("bad request", request=req, response=resp)

    with pytest.raises(httpx.HTTPStatusError):
        await client.with_retry("bad_request_op", lambda: op())


@pytest.mark.asyncio
async def test_with_retry_retries_request_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    API_URL = "https://api.braintrust.dev"
    RETRY_DELAY = 0.1
    EXPECTED_ATTEMPTS = 2

    cfg = MigrationConfig(retry_attempts=EXPECTED_ATTEMPTS - 1, retry_delay=RETRY_DELAY)
    client = BraintrustClient(
        BraintrustOrgConfig(api_key="x", url=cast(HttpUrl, HttpUrl(API_URL))),
        cfg,
        "test",
    )

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("braintrust_migrate.client.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("braintrust_migrate.client.random.uniform", lambda a, b: 0.0)

    calls = {"n": 0}

    async def op() -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("nope", request=httpx.Request("GET", "https://x"))
        return "ok"

    out = await client.with_retry("connect_error_op", lambda: op())
    assert out == "ok"
    assert calls["n"] == EXPECTED_ATTEMPTS
    assert len(sleeps) == 1
