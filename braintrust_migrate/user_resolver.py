"""Shared cross-org user resolution for ACL and group-member migration.

Both the ACL and group migrators need to map a source user to the matching
destination user by email (and optionally invite missing users). This logic was
previously duplicated near-verbatim in both migrators; it now lives here.

It also distinguishes a genuine 404 (user not found -> cache the negative result
so we don't refetch) from auth/network/5xx failures (transient -> log and do NOT
cache, so a later attempt can still succeed). The old code swallowed every
exception and cached ``None`` permanently, which turned a transient failure into
a permanent "no such user".
"""

from __future__ import annotations

import httpx
import structlog

from braintrust_migrate.client import BraintrustClient

logger = structlog.get_logger(__name__)


def _is_http_404(exc: Exception) -> bool:
    return (
        isinstance(exc, httpx.HTTPStatusError)
        and exc.response is not None
        and int(exc.response.status_code) == 404
    )


def _normalize_email(email: str) -> str:
    return email.strip().lower()


class UserResolver:
    """Resolve source users to destination users by email, with caching."""

    def __init__(
        self,
        source_client: BraintrustClient,
        dest_client: BraintrustClient,
        *,
        op_suffix: str = "",
    ) -> None:
        self.source_client = source_client
        self.dest_client = dest_client
        # Suffix appended to with_retry operation names so callers stay
        # distinguishable in telemetry (e.g. "_for_group_members").
        self._op_suffix = op_suffix
        self._source_user_email_cache: dict[str, str | None] = {}
        self._dest_user_id_by_email_cache: dict[str, str | None] = {}
        self._invited_user_emails: set[str] = set()
        self._logger = logger.bind(component="UserResolver")

    async def source_user_email(self, source_user_id: str) -> str | None:
        """Get a source user's (normalized) email by user id."""
        if source_user_id in self._source_user_email_cache:
            return self._source_user_email_cache[source_user_id]

        try:
            response = await self.source_client.with_retry(
                f"get_source_user{self._op_suffix}",
                lambda uid=source_user_id: self.source_client.raw_request(
                    "GET", f"/v1/user/{uid}"
                ),
            )
        except Exception as e:
            if _is_http_404(e):
                # Genuinely not found: cache the negative result.
                self._source_user_email_cache[source_user_id] = None
                return None
            # Transient/auth/network failure: do not cache, so a retry can work.
            self._logger.warning(
                "Failed to fetch source user email; not caching",
                source_user_id=source_user_id,
                error=str(e),
            )
            return None

        email = response.get("email") if isinstance(response, dict) else None
        email = (
            email.strip().lower()
            if isinstance(email, str) and email.strip()
            else None
        )
        self._source_user_email_cache[source_user_id] = email
        return email

    async def find_dest_user_id_by_email(
        self, email: str, *, force_refresh: bool = False
    ) -> str | None:
        """Find a destination user id by email."""
        normalized_email = _normalize_email(email)
        if force_refresh:
            self._dest_user_id_by_email_cache.pop(normalized_email, None)
        if normalized_email in self._dest_user_id_by_email_cache:
            return self._dest_user_id_by_email_cache[normalized_email]

        try:
            response = await self.dest_client.with_retry(
                f"list_dest_users_by_email{self._op_suffix}",
                lambda e=normalized_email: self.dest_client.raw_request(
                    "GET", "/v1/user", params={"email": e, "limit": 100}
                ),
            )
        except Exception as e:
            if _is_http_404(e):
                self._dest_user_id_by_email_cache[normalized_email] = None
                return None
            self._logger.warning(
                "Failed to list dest users by email; not caching",
                email=normalized_email,
                error=str(e),
            )
            return None

        if isinstance(response, dict):
            objects = response.get("objects", [])
        elif isinstance(response, list):
            objects = response
        else:
            objects = []

        dest_user_id = None
        for user in objects:
            if not isinstance(user, dict):
                continue
            user_email = user.get("email")
            user_id = user.get("id")
            if (
                isinstance(user_email, str)
                and user_email.strip().lower() == normalized_email
                and isinstance(user_id, str)
                and user_id
            ):
                dest_user_id = user_id
                break

        self._dest_user_id_by_email_cache[normalized_email] = dest_user_id
        return dest_user_id

    async def invite_user_to_dest_org(self, email: str) -> bool:
        """Invite a user to the destination org via the organization members API."""
        normalized_email = _normalize_email(email)
        if normalized_email in self._invited_user_emails:
            return True

        try:
            await self.dest_client.with_retry(
                f"invite_user_to_dest_org{self._op_suffix}",
                lambda e=normalized_email: self.dest_client.raw_request(
                    "PATCH",
                    "/v1/organization/members",
                    json={
                        "invite_users": {
                            "emails": [e],
                            "send_invite_emails": False,
                        }
                    },
                ),
            )
        except Exception as e:
            self._logger.warning(
                "Failed to invite user to dest org",
                email=normalized_email,
                error=str(e),
            )
            return False

        self._invited_user_emails.add(normalized_email)
        # Invalidate cache in case it was previously absent.
        self._dest_user_id_by_email_cache.pop(normalized_email, None)
        return True
