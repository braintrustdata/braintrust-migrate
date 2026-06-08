"""Group migrator for Braintrust migration tool."""

import asyncio

from braintrust_migrate.resources.base import ResourceMigrator
from braintrust_migrate.user_resolver import UserResolver


class GroupMigrator(ResourceMigrator[dict]):
    """Migrator for Braintrust groups.

    Handles group migration including group inheritance dependencies.
    Groups can inherit from other groups via the member_groups field,
    so parent groups must be migrated before child groups.

    Note: member_users can be migrated only when explicit user mapping is enabled.
    By default, member_users are skipped since users are organization-specific.

    Uses raw API requests instead of SDK to avoid model dependencies.
    """

    def __init__(self, source_client, dest_client, checkpoint_dir, batch_size: int = 100):
        super().__init__(source_client, dest_client, checkpoint_dir, batch_size=batch_size)
        self._user_resolver = UserResolver(
            self.source_client, self.dest_client, op_suffix="_for_group_members"
        )

    @property
    def resource_name(self) -> str:
        """Human-readable name for this resource type."""
        return "Groups"

    def _group_user_mapping_enabled(self) -> bool:
        """Return whether group member user mapping is enabled."""
        cfg = getattr(self.source_client, "migration_config", None) or getattr(
            self.dest_client, "migration_config", None
        )
        value = getattr(cfg, "group_map_users", False)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y", "on"}
        return False

    def _group_auto_invite_enabled(self) -> bool:
        """Return whether group member auto-invite fallback is enabled."""
        cfg = getattr(self.source_client, "migration_config", None) or getattr(
            self.dest_client, "migration_config", None
        )
        value = getattr(cfg, "group_auto_invite_users", False)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y", "on"}
        return False

    async def _resolve_group_member_user_id(self, source_user_id: str) -> str | None:
        """Resolve group member user_id by source/destination email matching."""
        existing_mapping = self.state.id_mapping.get(source_user_id)
        if existing_mapping:
            return existing_mapping

        source_email = await self._user_resolver.source_user_email(source_user_id)
        if not source_email:
            return None

        dest_user_id = await self._user_resolver.find_dest_user_id_by_email(
            source_email
        )
        if not dest_user_id and self._group_auto_invite_enabled():
            invited = await self._user_resolver.invite_user_to_dest_org(source_email)
            if invited:
                post_invite_attempts = 4
                for attempt in range(post_invite_attempts):
                    dest_user_id = await self._user_resolver.find_dest_user_id_by_email(
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

    async def get_dependencies(self, resource: dict) -> list[str]:
        """Get list of group IDs that this group depends on.

        Groups can depend on other groups via the member_groups field.

        Args:
            resource: Group dict to get dependencies for.

        Returns:
            List of group IDs this group inherits from.
        """
        dependencies = []

        # Check if group has member_groups (inheritance)
        member_groups = resource.get("member_groups")
        if member_groups:
            for group_id in member_groups:
                dependencies.append(group_id)
                self._logger.debug(
                    "Found group inheritance dependency",
                    group_id=resource.get("id"),
                    group_name=resource.get("name"),
                    parent_group_id=group_id,
                )

        return dependencies

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        """List all groups from the source organization using raw API.

        Args:
            project_id: Not used for groups (they are org-scoped).

        Returns:
            List of group dicts from the source organization.
        """
        try:
            # Groups are organization-scoped, not project-scoped
            # Use base class helper but without project_id
            return await self._list_resources_with_client(
                self.source_client, "groups", project_id=None
            )

        except Exception as e:
            self._logger.error("Failed to list source groups", error=str(e))
            raise

    async def migrate_resource(self, resource: dict) -> str:
        """Migrate a single group from source to destination using raw API.

        Args:
            resource: Source group dict to migrate.

        Returns:
            ID of the created group in destination.

        Raises:
            Exception: If migration fails.
        """
        self._logger.info(
            "Migrating group",
            source_id=resource.get("id"),
            name=resource.get("name"),
            org_id=resource.get("org_id"),
        )

        # Create group in destination using base class serialization
        create_params = self.serialize_resource_for_insert(resource)

        # Handle member_groups with dependency resolution
        member_groups = resource.get("member_groups")
        if member_groups:
            resolved_member_groups = []
            for group_id in member_groups:
                # Resolve group dependency to destination ID
                dest_group_id = self.state.id_mapping.get(group_id)
                if dest_group_id:
                    resolved_member_groups.append(dest_group_id)
                    self._logger.debug(
                        "Resolved group inheritance dependency",
                        group_id=resource.get("id"),
                        source_parent_group_id=group_id,
                        dest_parent_group_id=dest_group_id,
                    )
                else:
                    self._logger.warning(
                        "Could not resolve group inheritance dependency - parent group may not have been migrated",
                        group_id=resource.get("id"),
                        group_name=resource.get("name"),
                        source_parent_group_id=group_id,
                    )

            if resolved_member_groups:
                create_params["member_groups"] = resolved_member_groups

        member_users = resource.get("member_users")
        if member_users:
            if self._group_user_mapping_enabled():
                resolved_member_users: list[str] = []
                unresolved_source_user_ids: list[str] = []
                seen_dest_user_ids: set[str] = set()
                for source_user_id in member_users:
                    if not isinstance(source_user_id, str) or not source_user_id:
                        continue
                    mapped_user_id = await self._resolve_group_member_user_id(source_user_id)
                    if mapped_user_id:
                        if mapped_user_id not in seen_dest_user_ids:
                            resolved_member_users.append(mapped_user_id)
                            seen_dest_user_ids.add(mapped_user_id)
                    else:
                        unresolved_source_user_ids.append(source_user_id)

                if resolved_member_users:
                    create_params["member_users"] = resolved_member_users
                else:
                    create_params.pop("member_users", None)

                if unresolved_source_user_ids:
                    self._logger.warning(
                        "Skipping unresolved group member users",
                        group_id=resource.get("id"),
                        group_name=resource.get("name"),
                        unresolved_count=len(unresolved_source_user_ids),
                    )
            else:
                self._logger.info(
                    "Skipping member_users migration - users are organization-specific",
                    group_id=resource.get("id"),
                    group_name=resource.get("name"),
                    user_count=len(member_users),
                )
                # Remove member_users from create_params to avoid trying to migrate them
                create_params.pop("member_users", None)

        # Create group using raw API
        response = await self.dest_client.with_retry(
            "create_group",
            lambda create_params=create_params: self.dest_client.raw_request(
                "POST",
                "/v1/group",
                json=create_params,
            ),
        )

        dest_group_id = response.get("id")
        if not dest_group_id:
            raise ValueError(
                f"No ID returned when creating group {resource.get('name')}"
            )

        self._logger.info(
            "Created group in destination",
            source_id=resource.get("id"),
            dest_id=dest_group_id,
            name=resource.get("name"),
        )

        return dest_group_id
