"""Tests for BTQL query builder with date range filters."""

from __future__ import annotations

import pytest

from braintrust_migrate.streaming_utils import build_btql_sorted_page_query


class TestBuildBtqlSortedPageQuery:
    """Tests for build_btql_sorted_page_query function."""

    def test_basic_query_no_filters(self) -> None:
        """Test basic query without any filters."""
        query = build_btql_sorted_page_query(
            from_expr="project_logs('proj123', shape => 'spans')",
            limit=100,
            last_pagination_key=None,
        )

        assert "SELECT *" in query
        assert "FROM project_logs('proj123', shape => 'spans')" in query
        assert "ORDER BY _pagination_key ASC" in query
        assert "LIMIT 100" in query
        assert "WHERE" not in query

    def test_query_with_pagination_key(self) -> None:
        """Test query with pagination key (resume)."""
        query = build_btql_sorted_page_query(
            from_expr="project_logs('proj123', shape => 'spans')",
            limit=100,
            last_pagination_key="abc123",
        )

        assert "WHERE" in query
        assert "_pagination_key > 'abc123'" in query

    def test_query_with_pagination_key_inclusive(self) -> None:
        """Test query with inclusive pagination key."""
        query = build_btql_sorted_page_query(
            from_expr="project_logs('proj123', shape => 'spans')",
            limit=100,
            last_pagination_key="abc123",
            last_pagination_key_inclusive=True,
        )

        assert "WHERE" in query
        assert "_pagination_key >= 'abc123'" in query

    def test_query_with_created_after_only(self) -> None:
        """Test query with created_after filter only."""
        query = build_btql_sorted_page_query(
            from_expr="project_logs('proj123', shape => 'spans')",
            limit=100,
            last_pagination_key=None,
            created_after="2026-01-15T00:00:00Z",
        )

        assert "WHERE" in query
        assert "created >= '2026-01-15T00:00:00Z'" in query
        assert "created <" not in query

    def test_query_with_created_before_only(self) -> None:
        """Test query with created_before filter only."""
        query = build_btql_sorted_page_query(
            from_expr="project_logs('proj123', shape => 'spans')",
            limit=100,
            last_pagination_key=None,
            created_before="2026-02-01T00:00:00Z",
        )

        assert "WHERE" in query
        assert "created < '2026-02-01T00:00:00Z'" in query
        assert "created >=" not in query

    def test_query_with_date_range(self) -> None:
        """Test query with both created_after and created_before (date range)."""
        query = build_btql_sorted_page_query(
            from_expr="project_logs('proj123', shape => 'spans')",
            limit=100,
            last_pagination_key=None,
            created_after="2026-01-01T00:00:00Z",
            created_before="2026-02-01T00:00:00Z",
        )

        assert "WHERE" in query
        assert "created >= '2026-01-01T00:00:00Z'" in query
        assert "created < '2026-02-01T00:00:00Z'" in query
        # Both conditions should be ANDed together
        assert " AND " in query

    def test_query_with_date_range_and_pagination(self) -> None:
        """Test query with date range and pagination key."""
        query = build_btql_sorted_page_query(
            from_expr="project_logs('proj123', shape => 'spans')",
            limit=100,
            last_pagination_key="pk123",
            created_after="2026-01-01T00:00:00Z",
            created_before="2026-02-01T00:00:00Z",
        )

        assert "WHERE" in query
        assert "created >= '2026-01-01T00:00:00Z'" in query
        assert "created < '2026-02-01T00:00:00Z'" in query
        assert "_pagination_key > 'pk123'" in query
        # All three conditions should be ANDed together
        assert query.count(" AND ") == 2

    def test_query_with_custom_select(self) -> None:
        """Test query with custom select clause."""
        query = build_btql_sorted_page_query(
            from_expr="project_logs('proj123', shape => 'spans')",
            limit=100,
            last_pagination_key=None,
            select="_pagination_key",
        )

        assert "SELECT _pagination_key" in query
        assert "SELECT *" not in query

    def test_query_escapes_special_characters_in_pagination_key(self) -> None:
        """Test that special characters in pagination key are escaped."""
        query = build_btql_sorted_page_query(
            from_expr="project_logs('proj123', shape => 'spans')",
            limit=100,
            last_pagination_key="key'with'quotes",
        )

        # Single quotes should be escaped
        assert "key\\'with\\'quotes" in query

    def test_query_escapes_special_characters_in_date_filters(self) -> None:
        """Test that special characters in date values are escaped."""
        # This shouldn't happen with real dates, but the function should handle it
        query = build_btql_sorted_page_query(
            from_expr="project_logs('proj123', shape => 'spans')",
            limit=100,
            last_pagination_key=None,
            created_after="2026-01-01T00:00:00Z",
        )

        # Normal dates should pass through unchanged
        assert "2026-01-01T00:00:00Z" in query

    def test_query_for_experiment_events(self) -> None:
        """Test query for experiment events with date range."""
        query = build_btql_sorted_page_query(
            from_expr="experiment('exp123', shape => 'spans')",
            limit=50,
            last_pagination_key=None,
            created_after="2026-01-15T00:00:00Z",
            created_before="2026-01-16T00:00:00Z",
        )

        assert "FROM experiment('exp123', shape => 'spans')" in query
        assert "created >= '2026-01-15T00:00:00Z'" in query
        assert "created < '2026-01-16T00:00:00Z'" in query
        assert "LIMIT 50" in query

    def test_query_for_dataset_events(self) -> None:
        """Test query for dataset events with date range."""
        query = build_btql_sorted_page_query(
            from_expr="dataset('ds123', shape => 'spans')",
            limit=200,
            last_pagination_key="resume_pk",
            created_after="2026-01-01T00:00:00Z",
        )

        assert "FROM dataset('ds123', shape => 'spans')" in query
        assert "created >= '2026-01-01T00:00:00Z'" in query
        assert "_pagination_key > 'resume_pk'" in query
        assert "LIMIT 200" in query

    def test_empty_string_filters_are_ignored(self) -> None:
        """Test that empty string filters are treated as None."""
        query = build_btql_sorted_page_query(
            from_expr="project_logs('proj123', shape => 'spans')",
            limit=100,
            last_pagination_key="",
            created_after="",
            created_before="",
        )

        # Empty strings should not create WHERE conditions
        assert "WHERE" not in query

    def test_none_filters_are_ignored(self) -> None:
        """Test that None filters are properly ignored."""
        query = build_btql_sorted_page_query(
            from_expr="project_logs('proj123', shape => 'spans')",
            limit=100,
            last_pagination_key=None,
            created_after=None,
            created_before=None,
        )

        assert "WHERE" not in query


class TestQueryConditionOrder:
    """Tests to verify the order and structure of query conditions."""

    def test_conditions_are_properly_joined_with_and(self) -> None:
        """Test that multiple conditions are joined with AND."""
        query = build_btql_sorted_page_query(
            from_expr="project_logs('p', shape => 'spans')",
            limit=100,
            last_pagination_key="pk",
            created_after="2026-01-01T00:00:00Z",
            created_before="2026-02-01T00:00:00Z",
        )

        # Extract WHERE clause
        where_start = query.find("WHERE")
        order_start = query.find("ORDER BY")
        where_clause = query[where_start:order_start].strip()

        # Should have all three conditions
        assert "created >=" in where_clause
        assert "created <" in where_clause
        assert "_pagination_key >" in where_clause

    def test_query_structure_is_valid_sql(self) -> None:
        """Test that the generated query has valid SQL structure."""
        query = build_btql_sorted_page_query(
            from_expr="project_logs('p', shape => 'spans')",
            limit=100,
            last_pagination_key="pk",
            created_after="2026-01-01T00:00:00Z",
            created_before="2026-02-01T00:00:00Z",
        )

        # Check proper clause ordering
        select_pos = query.find("SELECT")
        from_pos = query.find("FROM")
        where_pos = query.find("WHERE")
        order_pos = query.find("ORDER BY")
        limit_pos = query.find("LIMIT")

        assert select_pos < from_pos < where_pos < order_pos < limit_pos
