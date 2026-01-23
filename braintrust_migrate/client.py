"""Braintrust API client wrapper with health checks and retry logic."""

import asyncio
import gzip
import json as _json
import random
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
import structlog
from braintrust_api import AsyncBraintrust

from braintrust_migrate.config import BraintrustOrgConfig, MigrationConfig

logger = structlog.get_logger(__name__)

# HTTP status codes
HTTP_STATUS_SEE_OTHER = 303


class BraintrustClientError(Exception):
    """Base exception for Braintrust client errors."""

    pass


class BraintrustConnectionError(BraintrustClientError):
    """Exception raised when connection to Braintrust API fails."""

    pass


class BraintrustAPIError(BraintrustClientError):
    """Exception raised when Braintrust API returns an error."""

    pass


class BraintrustClient:
    """Thin wrapper around braintrust-api-py AsyncClient with additional features.

    Provides:
    - Health checks and connectivity validation
    - Retry logic with exponential backoff
    - Structured logging
    - Connection pooling
    """

    def __init__(
        self,
        org_config: BraintrustOrgConfig,
        migration_config: MigrationConfig,
        org_name: str = "unknown",
    ) -> None:
        """Initialize the Braintrust client wrapper.

        Args:
            org_config: Organization configuration with API key and URL.
            migration_config: Migration configuration with retry settings.
            org_name: Human-readable name for this organization (source/dest).
        """
        self.org_config = org_config
        self.migration_config = migration_config
        self.org_name = org_name
        self._client: AsyncBraintrust | None = None
        self._http_client: httpx.AsyncClient | None = None
        self._logger = logger.bind(org=org_name, url=str(org_config.url))
        self._org_id: str | None = None

    async def __aenter__(self) -> "BraintrustClient":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()

    async def connect(self) -> None:
        """Establish connection to Braintrust API.

        Raises:
            BraintrustConnectionError: If connection fails.
        """
        if self._client is not None:
            return

        try:
            self._logger.info("Connecting to Braintrust API")

            # Create HTTP client for auxiliary requests
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=5),
            )

            # Create Braintrust API client
            self._client = AsyncBraintrust(
                api_key=self.org_config.api_key,
                base_url=str(self.org_config.url),
                http_client=self._http_client,
            )

            # Perform health check
            await self.health_check()

            self._logger.info("Successfully connected to Braintrust API")

        except Exception as e:
            self._logger.error("Failed to connect to Braintrust API", error=str(e))
            await self.close()
            raise BraintrustConnectionError(
                f"Failed to connect to {self.org_name}: {e}"
            ) from e

    async def close(self) -> None:
        """Close the connection to Braintrust API."""
        if self._client is not None:
            try:
                await self._client.close()
            except Exception as e:
                self._logger.warning("Error closing Braintrust client", error=str(e))
            finally:
                self._client = None

        if self._http_client is not None:
            try:
                await self._http_client.aclose()
            except Exception as e:
                self._logger.warning("Error closing HTTP client", error=str(e))
            finally:
                self._http_client = None

        self._logger.info("Closed connection to Braintrust API")

    async def raw_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Perform a raw HTTP request against the Braintrust API.

        This is useful for endpoints that are not ergonomically exposed by the
        generated `braintrust-api` client, or when we need tight control over
        request/response behavior (e.g. cursor-pagination for large logs).

        Args:
            method: HTTP method (GET/POST/etc).
            path: Absolute API path, e.g. '/v1/project_logs/{project_id}/fetch'.
            params: Optional query parameters.
            json: Optional JSON body.
            timeout: Optional per-request timeout override, in seconds.

        Returns:
            Parsed JSON response.
        """
        if self._http_client is None:
            raise BraintrustConnectionError(f"Not connected to {self.org_name}")

        base = str(self.org_config.url).rstrip("/")
        url = f"{base}{path}"

        # WARNING: The Braintrust API may respond with a 303 redirect to a signed S3
        # URL (e.g. ...json.gz) for large responses. We must not forward the
        # Braintrust Authorization header to non-Braintrust domains.
        base_origin = httpx.URL(base)

        headers: dict[str, str] = {"Authorization": f"Bearer {self.org_config.api_key}"}

        request_timeout = (
            httpx.Timeout(timeout) if timeout is not None else self._http_client.timeout
        )

        def _origin(u: httpx.URL) -> tuple[str, str, int | None]:
            return (u.scheme, u.host, u.port)

        current_method = method.upper()
        current_url = httpx.URL(url)
        current_params = params
        current_json = json
        max_redirects = 5

        for _ in range(max_redirects + 1):
            # Only send Authorization to the Braintrust API origin.
            req_headers = (
                headers if _origin(current_url) == _origin(base_origin) else {}
            )
            resp = await self._http_client.request(
                method=current_method,
                url=str(current_url),
                headers=req_headers,
                params=current_params,
                json=current_json,
                timeout=request_timeout,
                follow_redirects=False,
            )

            # Manually handle redirects to avoid leaking Authorization headers.
            if resp.status_code in (301, 302, 303, 307, 308):
                location = resp.headers.get("Location")
                if not location:
                    break
                next_url = current_url.join(location)
                current_url = next_url
                # 303 explicitly switches to GET, dropping request body.
                if resp.status_code == HTTP_STATUS_SEE_OTHER:
                    current_method = "GET"
                    current_params = None
                    current_json = None
                continue

            resp.raise_for_status()

            try:
                return resp.json()
            except Exception:
                # Some deployments redirect to a signed ...json.gz URL where the
                # payload is gzip-compressed but may not set Content-Encoding.
                content = resp.content
                if content[:2] == b"\x1f\x8b":
                    try:
                        decompressed = gzip.decompress(content)
                        return _json.loads(decompressed.decode("utf-8"))
                    except Exception:
                        pass
                raise

        # If we fell out due to a redirect loop or missing Location, raise.
        resp.raise_for_status()
        return resp.json()

    async def get_org_id(self) -> str:
        """Best-effort: get the org_id for this API key.

        Attachment APIs require an explicit org_id on some deployments.
        """
        if self._org_id:
            return self._org_id

        # The Braintrust SDK uses GET /ping (no /v1) on the api_url.
        candidates = ["/ping", "/v1/ping"]
        last_err: Exception | None = None
        for path in candidates:
            try:
                resp = await self.with_retry(
                    "ping", lambda p=path: self.raw_request("GET", p)
                )
                if isinstance(resp, dict):
                    for key in ("org_id", "orgId"):
                        v = resp.get(key)
                        if isinstance(v, str) and v:
                            self._org_id = v
                            return v
            except Exception as e:
                last_err = e
                continue

        raise BraintrustAPIError(
            "Unable to determine org_id for attachment operations. "
            "Tried /ping and /v1/ping; last error: "
            f"{last_err}"
        )

    @property
    def client(self) -> AsyncBraintrust:
        """Get the underlying Braintrust API client.

        Returns:
            The AsyncBraintrust client instance.

        Raises:
            BraintrustConnectionError: If not connected.
        """
        if self._client is None:
            raise BraintrustConnectionError(f"Not connected to {self.org_name}")
        return self._client

    async def health_check(self) -> dict[str, Any]:
        """Perform health check against the Braintrust API.

        Returns:
            Health check response data.

        Raises:
            BraintrustConnectionError: If health check fails.
        """
        if self._client is None:
            raise BraintrustConnectionError(f"Not connected to {self.org_name}")

        try:
            # Try to list projects as a health check
            await self._client.projects.list(limit=1)

            health_data = {
                "status": "healthy",
                "url": str(self.org_config.url),
                "projects_accessible": True,
                "api_version": "v1",  # Braintrust API version
            }

            self._logger.debug("Health check passed", health_data=health_data)
            return health_data

        except Exception as e:
            self._logger.error("Health check failed", error=str(e))
            raise BraintrustConnectionError(
                f"Health check failed for {self.org_name}: {e}"
            ) from e

    async def check_brainstore_enabled(self) -> bool:
        """Check if Brainstore is enabled for this organization.

        Returns:
            True if Brainstore is enabled, False otherwise.
        """
        try:
            # This is a placeholder - actual implementation would depend on
            # the Braintrust API's way of exposing Brainstore status
            # For now, we'll assume it's enabled
            self._logger.debug("Checking Brainstore status")
            return True
        except Exception as e:
            self._logger.warning("Could not determine Brainstore status", error=str(e))
            return False

    async def with_retry(self, operation_name: str, coro_func):
        """Execute a coroutine function with adaptive retry logic.

        Args:
            operation_name: Human-readable name for the operation.
            coro_func: Callable that returns a coroutine or AsyncPaginator to execute.

        Returns:
            Result of the coroutine or AsyncPaginator.

        Raises:
            BraintrustAPIError: If all retry attempts fail.
        """
        attempts = int(self.migration_config.retry_attempts) + 1
        base_delay = float(self.migration_config.retry_delay)
        max_delay = 60.0

        HTTP_STATUS_TOO_MANY_REQUESTS = 429

        def _parse_retry_after_seconds(resp: httpx.Response) -> float | None:
            raw = resp.headers.get("Retry-After")
            if not raw:
                return None
            raw = raw.strip()
            # Most common: integer seconds
            try:
                sec = float(raw)
                if sec >= 0:
                    return sec
            except ValueError:
                pass
            # RFC 7231 date format fallback
            try:
                dt = parsedate_to_datetime(raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                now = datetime.now(UTC)
                sec = (dt - now).total_seconds()
                if sec >= 0:
                    return sec
            except Exception:
                return None
            return None

        def _classify_exception(
            exc: Exception,
        ) -> tuple[bool, int | None, float | None]:
            """Return (retryable, status_code, retry_after_seconds)."""
            if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
                status = int(exc.response.status_code)
                retry_after = _parse_retry_after_seconds(exc.response)
                if status == HTTP_STATUS_TOO_MANY_REQUESTS:
                    return True, status, retry_after
                if status in {408, 409, 425, 500, 502, 503, 504}:
                    return True, status, retry_after
                return False, status, retry_after
            if isinstance(exc, httpx.RequestError):
                # Network/DNS/timeouts/etc.
                return True, None, None
            return False, None, None

        def _exc_context(exc: Exception) -> dict[str, Any]:
            ctx: dict[str, Any] = {
                "error_type": type(exc).__name__,
                "error_repr": repr(exc),
            }
            if isinstance(exc, httpx.HTTPStatusError):
                if exc.request is not None:
                    ctx["request_method"] = exc.request.method
                    ctx["request_url"] = str(exc.request.url)
                if exc.response is not None:
                    ctx["response_status_code"] = int(exc.response.status_code)
                    # Best-effort small excerpt of response body to help debug 4xx/5xx
                    try:
                        text = exc.response.text
                        ctx["response_text_excerpt"] = text[:500]
                    except Exception:
                        pass
            elif isinstance(exc, httpx.RequestError):
                if exc.request is not None:
                    ctx["request_method"] = exc.request.method
                    ctx["request_url"] = str(exc.request.url)
            return ctx

        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                self._logger.debug(
                    "Executing operation",
                    operation=operation_name,
                    attempt=attempt,
                    max_attempts=attempts,
                )
                result = coro_func()
                if hasattr(result, "__await__"):
                    result = await result
                return result
            except Exception as e:
                last_exc = e
                retryable, status, retry_after = _classify_exception(e)
                exc_ctx = _exc_context(e)

                if not retryable or attempt >= attempts:
                    self._logger.error(
                        "Operation failed",
                        operation=operation_name,
                        attempt=attempt,
                        max_attempts=attempts,
                        status_code=status,
                        error=str(e),
                        **exc_ctx,
                    )
                    raise

                exp_delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
                # small jitter to avoid thundering herd
                jitter = random.uniform(0, min(1.0, exp_delay * 0.1))
                delay = min(max_delay, exp_delay + jitter)
                if retry_after is not None:
                    delay = min(max_delay, max(delay, retry_after))

                # Concise message for rate limits (expected behavior), verbose for other errors
                if status == HTTP_STATUS_TOO_MANY_REQUESTS:
                    self._logger.warning(
                        "Rate limited, retrying",
                        operation=operation_name,
                        retry_in_seconds=round(delay, 1),
                        attempt=f"{attempt}/{attempts}",
                    )
                else:
                    self._logger.warning(
                        "Operation failed, backing off and retrying",
                        operation=operation_name,
                        attempt=attempt,
                        max_attempts=attempts,
                        status_code=status,
                        retry_after_seconds=retry_after,
                        sleep_seconds=delay,
                        error=str(e),
                        **exc_ctx,
                    )
                await asyncio.sleep(delay)

        # Should be unreachable
        if last_exc is not None:
            raise last_exc
        raise BraintrustAPIError(f"{operation_name} failed with unknown error")


@asynccontextmanager
async def create_client_pair(
    source_config: BraintrustOrgConfig,
    dest_config: BraintrustOrgConfig,
    migration_config: MigrationConfig,
) -> AsyncGenerator[tuple[BraintrustClient, BraintrustClient], None]:
    """Create a pair of connected Braintrust clients for source and destination.

    Args:
        source_config: Source organization configuration.
        dest_config: Destination organization configuration.
        migration_config: Migration configuration.

    Yields:
        Tuple of (source_client, dest_client).

    Raises:
        BraintrustConnectionError: If either client fails to connect.
    """
    source_client = BraintrustClient(source_config, migration_config, "source")
    dest_client = BraintrustClient(dest_config, migration_config, "destination")

    try:
        # Connect both clients concurrently
        await asyncio.gather(
            source_client.connect(),
            dest_client.connect(),
        )

        # Verify both have compatible API versions and Brainstore if needed
        source_health = await source_client.health_check()
        dest_health = await dest_client.health_check()

        logger.info(
            "Successfully connected to both organizations",
            source_health=source_health,
            dest_health=dest_health,
        )

        yield source_client, dest_client

    finally:
        # Close both clients
        await asyncio.gather(
            source_client.close(),
            dest_client.close(),
            return_exceptions=True,
        )
