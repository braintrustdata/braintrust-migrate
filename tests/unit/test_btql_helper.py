from __future__ import annotations

from typing import Any

import httpx
import pytest

from braintrust_migrate.btql import fetch_btql_sorted_page_with_retries


class _StubBtqlClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.mode: str = "ok"
        self.with_retry_calls: list[dict[str, Any]] = []

    async def with_retry(
        self,
        _operation_name: str,
        coro_func,
        *,
        non_retryable_statuses: set[int] | None = None,
    ):
        self.with_retry_calls.append(
            {
                "non_retryable_statuses": set(non_retryable_statuses or set()),
            }
        )
        while True:
            try:
                res = coro_func()
                if hasattr(res, "__await__"):
                    return await res
                return res
            except httpx.HTTPStatusError as exc:
                status = (
                    int(exc.response.status_code) if exc.response is not None else None
                )
                if (
                    status is not None
                    and non_retryable_statuses is not None
                    and status in non_retryable_statuses
                ):
                    raise
                if status == 429:
                    continue
                raise

    async def raw_request(
        self,
        method: str,
        path: str,
        *,
        json: Any | None = None,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> Any:
        _ = kwargs
        assert method.upper() == "POST"
        assert path == "/btql"
        assert json is not None
        assert timeout is not None
        self.calls.append(json)

        q = json.get("query")
        assert isinstance(q, str)

        if self.mode == "504_then_ok":
            if len(self.calls) == 1:
                req = httpx.Request("POST", "https://api.braintrust.dev/btql")
                resp = httpx.Response(504, request=req, text="timeout")
                raise httpx.HTTPStatusError("timeout", request=req, response=resp)
            return {"data": [{"_pagination_key": "p1"}]}

        if self.mode == "500_on_1000_then_ok":
            if "LIMIT 1000" in q:
                req = httpx.Request("POST", "https://api.braintrust.dev/btql")
                resp = httpx.Response(500, request=req, text="internal error")
                raise httpx.HTTPStatusError("internal", request=req, response=resp)
            return {"data": [{"_pagination_key": "p1"}]}

        if self.mode == "504_on_1000_then_ok":
            if "LIMIT 1000" in q:
                req = httpx.Request("POST", "https://api.braintrust.dev/btql")
                resp = httpx.Response(504, request=req, text="timeout")
                raise httpx.HTTPStatusError("timeout", request=req, response=resp)
            return {"data": [{"_pagination_key": "p1"}]}

        return {"data": [{"_pagination_key": "p1"}]}


@pytest.mark.asyncio
async def test_btql_helper_does_not_fallback_off_brainstore_on_504() -> None:
    c = _StubBtqlClient()
    c.mode = "504_then_ok"

    def q(limit: int) -> str:
        return f"SELECT * FROM project_logs('p', shape => 'spans') ORDER BY _pagination_key ASC LIMIT {limit}"

    with pytest.raises(httpx.HTTPStatusError):
        # configured_limit=10 has no smaller limits to try (floor_limit=25),
        # so a 504 will surface rather than switching away from Brainstore.
        await fetch_btql_sorted_page_with_retries(
            client=c,  # type: ignore[arg-type]
            query_for_limit=q,
            configured_limit=10,
            operation="btql_test",
            log_fields={"x": "y"},
        )

    assert len(c.calls) == 1
    assert c.calls[0]["use_brainstore"] is True


@pytest.mark.asyncio
async def test_btql_helper_retries_500_with_smaller_limit() -> None:
    c = _StubBtqlClient()
    c.mode = "500_on_1000_then_ok"

    def q(limit: int) -> str:
        return f"SELECT * FROM project_logs('p', shape => 'spans') ORDER BY _pagination_key ASC LIMIT {limit}"

    out = await fetch_btql_sorted_page_with_retries(
        client=c,  # type: ignore[arg-type]
        query_for_limit=q,
        configured_limit=1000,
        operation="btql_test",
        log_fields={"x": "y"},
    )

    assert out["btql_last_pagination_key"] == "p1"
    queries = [call["query"] for call in c.calls]
    assert sum("LIMIT 1000" in qq for qq in queries) == 1
    assert sum("LIMIT 500" in qq for qq in queries) == 1
    assert c.with_retry_calls[0]["non_retryable_statuses"] == {500, 504}


@pytest.mark.asyncio
async def test_btql_helper_retries_504_with_smaller_limit_if_timeout_persists() -> None:
    c = _StubBtqlClient()
    c.mode = "504_on_1000_then_ok"

    def q(limit: int) -> str:
        return f"SELECT * FROM project_logs('p', shape => 'spans') ORDER BY _pagination_key ASC LIMIT {limit}"

    out = await fetch_btql_sorted_page_with_retries(
        client=c,  # type: ignore[arg-type]
        query_for_limit=q,
        configured_limit=1000,
        operation="btql_test",
        log_fields={"x": "y"},
    )

    assert out["btql_last_pagination_key"] == "p1"
    queries = [call["query"] for call in c.calls]
    assert sum("LIMIT 1000" in qq for qq in queries) == 1
    assert sum("LIMIT 500" in qq for qq in queries) == 1


@pytest.mark.asyncio
async def test_btql_helper_still_retries_429_at_same_limit() -> None:
    class _RateLimitOnceClient(_StubBtqlClient):
        def __init__(self) -> None:
            super().__init__()
            self.rate_limited = False

        async def raw_request(
            self,
            method: str,
            path: str,
            *,
            json: Any | None = None,
            timeout: float | None = None,
            **kwargs: Any,
        ) -> Any:
            _ = kwargs
            assert method.upper() == "POST"
            assert path == "/btql"
            assert json is not None
            assert timeout is not None
            self.calls.append(json)
            if not self.rate_limited:
                self.rate_limited = True
                req = httpx.Request("POST", "https://api.braintrust.dev/btql")
                resp = httpx.Response(429, request=req, text="rate limit")
                raise httpx.HTTPStatusError("rate limit", request=req, response=resp)
            return {"data": [{"_pagination_key": "p1"}]}

    c = _RateLimitOnceClient()

    def q(limit: int) -> str:
        return f"SELECT * FROM project_logs('p', shape => 'spans') ORDER BY _pagination_key ASC LIMIT {limit}"

    out = await fetch_btql_sorted_page_with_retries(
        client=c,  # type: ignore[arg-type]
        query_for_limit=q,
        configured_limit=1000,
        operation="btql_test",
        log_fields={"x": "y"},
    )

    assert out["btql_last_pagination_key"] == "p1"
    queries = [call["query"] for call in c.calls]
    assert sum("LIMIT 1000" in qq for qq in queries) == 2


@pytest.mark.asyncio
async def test_btql_helper_resets_to_configured_limit_on_next_fetch() -> None:
    class _Fail1000Then500Client(_StubBtqlClient):
        async def raw_request(
            self,
            method: str,
            path: str,
            *,
            json: Any | None = None,
            timeout: float | None = None,
            **kwargs: Any,
        ) -> Any:
            _ = kwargs
            assert method.upper() == "POST"
            assert path == "/btql"
            assert json is not None
            assert timeout is not None
            self.calls.append(json)

            q = json.get("query")
            assert isinstance(q, str)
            if "LIMIT 1000" in q:
                req = httpx.Request("POST", "https://api.braintrust.dev/btql")
                resp = httpx.Response(500, request=req, text="internal error")
                raise httpx.HTTPStatusError("internal", request=req, response=resp)
            return {"data": [{"_pagination_key": "p1"}]}

    c = _Fail1000Then500Client()

    def q(limit: int) -> str:
        return f"SELECT * FROM project_logs('p', shape => 'spans') ORDER BY _pagination_key ASC LIMIT {limit}"

    out1 = await fetch_btql_sorted_page_with_retries(
        client=c,  # type: ignore[arg-type]
        query_for_limit=q,
        configured_limit=1000,
        operation="btql_test",
        log_fields={"x": "y"},
    )
    out2 = await fetch_btql_sorted_page_with_retries(
        client=c,  # type: ignore[arg-type]
        query_for_limit=q,
        configured_limit=1000,
        operation="btql_test",
        log_fields={"x": "y"},
    )

    assert out1["btql_last_pagination_key"] == "p1"
    assert out2["btql_last_pagination_key"] == "p1"
    queries = [call["query"] for call in c.calls]
    assert [("LIMIT 1000" in qq, "LIMIT 500" in qq) for qq in queries] == [
        (True, False),
        (False, True),
        (True, False),
        (False, True),
    ]
