from __future__ import annotations

import pytest

from braintrust_migrate.config import canonicalize_created_after


def test_created_after_date_only_is_midnight_utc() -> None:
    assert canonicalize_created_after("2020-01-02") == "2020-01-02T00:00:00Z"


def test_created_after_accepts_z_suffix() -> None:
    assert canonicalize_created_after("2020-01-02T03:04:05Z") == "2020-01-02T03:04:05Z"


def test_created_after_converts_offset_to_utc() -> None:
    # 03:00 at -05:00 is 08:00Z
    assert (
        canonicalize_created_after("2020-01-02T03:00:00-05:00")
        == "2020-01-02T08:00:00Z"
    )


def test_created_after_naive_datetime_treated_as_utc() -> None:
    assert canonicalize_created_after("2020-01-02T03:04:05") == "2020-01-02T03:04:05Z"


def test_created_after_rejects_empty() -> None:
    with pytest.raises(ValueError, match="created_after cannot be empty"):
        canonicalize_created_after("   ")


def test_created_after_rejects_invalid() -> None:
    with pytest.raises(ValueError, match="Invalid created_after datetime"):
        canonicalize_created_after("not-a-date")
