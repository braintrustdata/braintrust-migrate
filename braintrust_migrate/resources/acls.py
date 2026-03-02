"""ACL migrator for Braintrust migration tool."""

import asyncio
from typing import Any

from braintrust_migrate.resources.base import (
    DEFAULT_LIST_PAGE_SIZE,
    MigrationResult,
    ResourceMigrator,
)


class ACLMigrator(ResourceMigrator[dict]):
    """Migrator for Braintrust ACLs.

    Handles ACL migration including complex dependency resolution:
    - object_id references to various resource types (projects, experiments, datasets, etc.)
    - role_id references to roles
    - group_id references to groups

    Note: user_id references are handled specially since users are
    organization-specific and cannot be migrated between organizations.
    By default, ACLs with user_id are skipped unless user mapping is enabled.

    Uses raw API requests instead of SDK to avoid model dependencies.
    """

    @property
    def resource_name(self) -> str:
        """Human-readable name for this resource type."""
        return "ACLs"

    def __init__(self, source_client, dest_client, checkpoint_dir, batch_size: int = 100):
        super().__init__(source_client, dest_client, checkpoint_dir, batch_size=batch_size)
        self._source_user_email_cache: dict[str, str | None] = {}
        self._dest_user_id_by_email_cache: dict[str, str | None] = {}
        self._invited_user_emails: set[str] = set()

    @property
    def supported_object_types(self) -> set[str]:
        """Object types this migrator can remap across organizations."""
        return {
            "organization",
            "project",
            "experiment",
            "dataset",
            "prompt",
            "group",
            "role",
        }

    async def get_dependencies(self, resource: dict) -> list[str]:
        """Get list of resource IDs that this ACL depends on.

        ACLs can depend on:
        - object_id: The resource the ACL applies to
        - role_id: The role being granted (if not a direct permission)
        - group_id: The group receiving the ACL (if not a user)

        Args:
            resource: ACL dict to get dependencies for.

        Returns:
            List of resource IDs this ACL depends on.
        """
        dependencies = []

        # Add object dependency
        object_id = resource.get("object_id")
        if object_id:
            dependencies.append(object_id)
            self._logger.debug(
                "Found ACL object dependency",
                acl_id=resource.get("id"),
                object_type=resource.get("object_type"),
                object_id=object_id,
            )

        # Add role dependency if present
        role_id = resource.get("role_id")
        if role_id:
            dependencies.append(role_id)
            self._logger.debug(
                "Found ACL role dependency",
                acl_id=resource.get("id"),
                role_id=role_id,
            )

        # Add group dependency if present
        group_id = resource.get("group_id")
        if group_id:
            dependencies.append(group_id)
            self._logger.debug(
                "Found ACL group dependency",
                acl_id=resource.get("id"),
                group_id=group_id,
            )

        return dependencies

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        """List all ACLs from the source organization.

        Args:
            project_id: Not used for ACLs (they are org-scoped).

        Returns:
            List of ACL dicts from the source organization.
        """
        try:
            if project_id is not None:
                self._logger.debug(
                    "Ignoring project_id for ACL list; ACLs are organization-scoped",
                    project_id=project_id,
                )

            all_acls: list[dict[str, Any]] = []
            starting_after: str | None = None
            page_num = 0

            while True:
                page_num += 1
                params: dict[str, Any] = {"limit": DEFAULT_LIST_PAGE_SIZE}
                if starting_after is not None:
                    params["starting_after"] = starting_after

                response = await self.source_client.with_retry(
                    "list_org_acls",
                    lambda p=params: self.source_client.raw_request(
                        "GET",
                        "/v1/acl/list_org",
                        params=p,
                    ),
                )

                if isinstance(response, list):
                    page_acls = [item for item in response if isinstance(item, dict)]
                elif isinstance(response, dict):
                    objects = response.get("objects", [])
                    page_acls = [item for item in objects if isinstance(item, dict)]
                else:
                    raise TypeError(
                        f"Unexpected ACL list response type: {type(response).__name__}"
                    )

                if not page_acls:
                    break

                all_acls.extend(page_acls)
                self._logger.info(
                    "Fetched ACL page",
                    page_num=page_num,
                    page_size=len(page_acls),
                    total_fetched=len(all_acls),
                )

                if len(page_acls) < DEFAULT_LIST_PAGE_SIZE:
                    break

                last_acl_id = page_acls[-1].get("id")
                if not isinstance(last_acl_id, str) or not last_acl_id:
                    self._logger.warning(
                        "Stopping ACL pagination due to missing cursor id",
                        page_num=page_num,
                    )
                    break
                starting_after = last_acl_id

            return all_acls

        except Exception as e:
            self._logger.error("Failed to list source ACLs", error=str(e))
            raise

    async def _resolve_acl_object_id(self, resource: dict) -> str | None:
        """Resolve an ACL object_id from source to destination context."""
        object_type = resource.get("object_type")
        object_id = resource.get("object_id")

        if object_type == "organization":
            return await self.dest_client.get_org_id()

        if not object_id:
            return None

        mapped_object_id = self.state.id_mapping.get(object_id)
        if mapped_object_id:
            return mapped_object_id

        # Safe fallback for object types with org-unique naming semantics.
        safe_resource_types = {
            "project": "projects",
            "group": "groups",
            "role": "roles",
        }
        dependency_resource_type = safe_resource_types.get(object_type)
        if dependency_resource_type:
            return await self.ensure_dependency_mapping(
                dependency_resource_type,
                object_id,
                project_id=None,
            )

        return None

    def _acl_user_mapping_enabled(self) -> bool:
        """Return whether ACL user mapping is enabled."""
        cfg = getattr(self.source_client, "migration_config", None) or getattr(
            self.dest_client, "migration_config", None
        )
        value = getattr(cfg, "acl_map_users", False)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y", "on"}
        return False

    def _acl_auto_invite_enabled(self) -> bool:
        """Return whether ACL auto-invite fallback is enabled."""
        cfg = getattr(self.source_client, "migration_config", None) or getattr(
            self.dest_client, "migration_config", None
        )
        value = getattr(cfg, "acl_auto_invite_users", False)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y", "on"}
        return False

    async def _get_source_user_email(self, source_user_id: str) -> str | None:
        """Get source user email by user ID."""
        if source_user_id in self._source_user_email_cache:
            return self._source_user_email_cache[source_user_id]

        try:
            response = await self.source_client.with_retry(
                "get_source_user",
                lambda uid=source_user_id: self.source_client.raw_request(
                    "GET",
                    f"/v1/user/{uid}",
                ),
            )
            email = response.get("email") if isinstance(response, dict) else None
            email = email.strip().lower() if isinstance(email, str) and email.strip() else None
            self._source_user_email_cache[source_user_id] = email
            return email
        except Exception:
            self._source_user_email_cache[source_user_id] = None
            return None

    async def _find_dest_user_id_by_email(
        self, email: str, *, force_refresh: bool = False
    ) -> str | None:
        """Find destination user ID by email."""
        normalized_email = email.strip().lower()
        if force_refresh:
            self._dest_user_id_by_email_cache.pop(normalized_email, None)
        if normalized_email in self._dest_user_id_by_email_cache:
            return self._dest_user_id_by_email_cache[normalized_email]

        try:
            response = await self.dest_client.with_retry(
                "list_dest_users_by_email",
                lambda e=normalized_email: self.dest_client.raw_request(
                    "GET",
                    "/v1/user",
                    params={"email": e, "limit": 100},
                ),
            )

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
        except Exception:
            self._dest_user_id_by_email_cache[normalized_email] = None
            return None

    async def _invite_user_to_dest_org(self, email: str) -> bool:
        """Invite a user to destination org via organization members API."""
        normalized_email = email.strip().lower()
        if normalized_email in self._invited_user_emails:
            return True

        try:
            await self.dest_client.with_retry(
                "invite_user_to_dest_org",
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
            self._invited_user_emails.add(normalized_email)
            # Invalidate cache in case it was previously absent.
            self._dest_user_id_by_email_cache.pop(normalized_email, None)
            return True
        except Exception:
            return False

    async def _resolve_acl_user_id(self, source_user_id: str) -> str | None:
        """Resolve ACL user_id by source/destination email matching."""
        existing_mapping = self.state.id_mapping.get(source_user_id)
        if existing_mapping:
            return existing_mapping

        source_email = await self._get_source_user_email(source_user_id)
        if not source_email:
            return None

        dest_user_id = await self._find_dest_user_id_by_email(source_email)
        if not dest_user_id and self._acl_auto_invite_enabled():
            invited = await self._invite_user_to_dest_org(source_email)
            if invited:
                # Membership propagation may be eventual; retry lookup briefly.
                post_invite_attempts = 4
                for attempt in range(post_invite_attempts):
                    dest_user_id = await self._find_dest_user_id_by_email(
                        source_email,
                        force_refresh=True,
                    )
                    if dest_user_id:
                        break
                    if attempt < post_invite_attempts - 1:
                        await asyncio.sleep(0.5 * (attempt + 1))

        if dest_user_id:
            self.state.id_mapping[source_user_id] = dest_user_id
            return dest_user_id
        return None

    async def _build_create_params(self, resource: dict) -> tuple[dict[str, Any] | None, str | None]:
        """Build destination create payload or return a skip reason."""
        object_type = resource.get("object_type")
        object_id = resource.get("object_id")
        user_id = resource.get("user_id")

        skip_reason: str | None = None
        if user_id and not self._acl_user_mapping_enabled():
            skip_reason = "user_acl_mapping_disabled"
        elif object_type not in self.supported_object_types:
            skip_reason = "unsupported_object_type"

        if skip_reason is not None:
            return None, skip_reason

        create_params = self.serialize_resource_for_insert(resource)
        mapped_object_id = await self._resolve_acl_object_id(resource)
        if mapped_object_id:
            create_params["object_id"] = mapped_object_id
        else:
            self._logger.warning(
                "Skipping ACL due to unresolved object dependency",
                acl_id=resource.get("id"),
                object_type=object_type,
                object_id=object_id,
            )
            skip_reason = "unresolved_object_dependency"

        if skip_reason is None and user_id:
            mapped_user_id = await self._resolve_acl_user_id(user_id)
            if mapped_user_id:
                create_params["user_id"] = mapped_user_id
                create_params.pop("group_id", None)
            else:
                self._logger.warning(
                    "Skipping ACL due to unresolved user dependency",
                    acl_id=resource.get("id"),
                    user_id=user_id,
                )
                skip_reason = "unresolved_user_dependency"

        if skip_reason is None and not user_id:
            group_id = resource.get("group_id")
            if group_id:
                mapped_group_id = self.state.id_mapping.get(group_id)
                if not mapped_group_id:
                    mapped_group_id = await self.ensure_dependency_mapping(
                        "groups", group_id, project_id=None
                    )
                if not mapped_group_id:
                    self._logger.warning(
                        "Skipping ACL due to unresolved group dependency",
                        acl_id=resource.get("id"),
                        group_id=group_id,
                    )
                    skip_reason = "unresolved_group_dependency"
                else:
                    create_params["group_id"] = mapped_group_id

        role_id = resource.get("role_id")
        if skip_reason is None and role_id:
            mapped_role_id = self.state.id_mapping.get(role_id)
            if not mapped_role_id:
                mapped_role_id = await self.ensure_dependency_mapping(
                    "roles", role_id, project_id=None
                )
            if not mapped_role_id:
                self._logger.warning(
                    "Skipping ACL due to unresolved role dependency",
                    acl_id=resource.get("id"),
                    role_id=role_id,
                )
                skip_reason = "unresolved_role_dependency"
            else:
                create_params["role_id"] = mapped_role_id

        if skip_reason is not None:
            return None, skip_reason
        return create_params, None

    async def _acls_equivalent(self, source_acl: dict, dest_acl: dict) -> bool:
        """Check if two ACLs are equivalent after ID mapping.

        Args:
            source_acl: Source ACL dict.
            dest_acl: Destination ACL dict.

        Returns:
            True if ACLs are equivalent, False otherwise.
        """
        # Check object_type and object_id (with mapping)
        source_object_id = source_acl.get("object_id")
        mapped_object_id = (
            self.state.id_mapping.get(source_object_id) if source_object_id else None
        )
        object_match = source_acl.get("object_type") == dest_acl.get(
            "object_type"
        ) and mapped_object_id == dest_acl.get("object_id")

        if not object_match:
            return False

        # Check user_id or group_id (with mapping for groups)
        user_group_match = True
        source_user_id = source_acl.get("user_id")
        source_group_id = source_acl.get("group_id")
        if source_user_id:
            mapped_user_id = self.state.id_mapping.get(source_user_id)
            user_group_match = mapped_user_id == dest_acl.get("user_id")
        elif source_group_id:
            mapped_group_id = self.state.id_mapping.get(source_group_id)
            user_group_match = mapped_group_id == dest_acl.get("group_id")
        else:
            # Neither user_id nor group_id - this shouldn't happen
            user_group_match = False

        if not user_group_match:
            return False

        # Check permission or role_id (with mapping for roles)
        permission_role_match = True
        source_permission = source_acl.get("permission")
        source_role_id = source_acl.get("role_id")
        if source_permission:
            permission_role_match = source_permission == dest_acl.get("permission")
        elif source_role_id:
            mapped_role_id = self.state.id_mapping.get(source_role_id)
            permission_role_match = mapped_role_id == dest_acl.get("role_id")

        if not permission_role_match:
            return False

        # Check restrict_object_type
        return source_acl.get("restrict_object_type") == dest_acl.get(
            "restrict_object_type"
        )

    async def migrate_resource(self, resource: dict) -> str:
        """Migrate a single ACL from source to destination using raw API.

        Args:
            resource: Source ACL dict to migrate.

        Returns:
            ID of the created ACL in destination.

        Raises:
            Exception: If migration fails.
        """
        resource_id = resource.get("id")
        object_id = resource.get("object_id")
        object_type = resource.get("object_type")

        self._logger.info(
            "Migrating ACL",
            source_id=resource_id,
            object_type=object_type,
            object_id=object_id,
        )

        create_params, skip_reason = await self._build_create_params(resource)
        if create_params is None:
            raise ValueError(
                f"ACL {resource_id} is not migratable (reason={skip_reason})"
            )

        # Create ACL using raw API
        response = await self.dest_client.with_retry(
            "create_acl",
            lambda create_params=create_params: self.dest_client.raw_request(
                "POST",
                "/v1/acl",
                json=create_params,
            ),
        )

        dest_acl_id = response.get("id")
        if not dest_acl_id:
            raise ValueError(
                f"No ID returned when creating ACL for {object_type}/{object_id}"
            )

        self._logger.info(
            "Created ACL in destination",
            source_id=resource_id,
            dest_id=dest_acl_id,
            object_type=object_type,
        )

        return dest_acl_id

    async def migrate_batch(self, resources: list[dict]) -> list[MigrationResult]:
        """Migrate a batch of ACLs with explicit skip handling."""
        results: list[MigrationResult] = []

        for resource in resources:
            source_id = self.get_resource_id(resource)
            if source_id in self.state.id_mapping:
                dest_id = self.state.id_mapping.get(source_id)
                results.append(
                    MigrationResult(
                        success=True,
                        source_id=source_id,
                        dest_id=dest_id,
                        skipped=True,
                        metadata={"skip_reason": "already_migrated"},
                    )
                )
                continue

            create_params, skip_reason = await self._build_create_params(resource)
            if create_params is None:
                results.append(
                    MigrationResult(
                        success=True,
                        source_id=source_id,
                        skipped=True,
                        metadata={"skip_reason": skip_reason or "not_migratable"},
                    )
                )
                continue

            try:
                dest_id = await self.dest_client.with_retry(
                    "create_acl",
                    lambda create_params=create_params: self.dest_client.raw_request(
                        "POST",
                        "/v1/acl",
                        json=create_params,
                    ),
                )
                dest_acl_id = dest_id.get("id")
                if not isinstance(dest_acl_id, str) or not dest_acl_id:
                    raise ValueError(
                        f"No ID returned when creating ACL for source {source_id}"
                    )

                self.record_success(source_id, dest_acl_id, resource)
                results.append(
                    MigrationResult(
                        success=True,
                        source_id=source_id,
                        dest_id=dest_acl_id,
                    )
                )
            except Exception as e:
                error_msg = f"Failed to migrate resource: {e}"
                self.record_failure(source_id, error_msg)
                results.append(
                    MigrationResult(
                        success=False,
                        source_id=source_id,
                        error=error_msg,
                    )
                )

        return results
