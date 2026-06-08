"""The console summary points to the JSON report for per-item skip detail."""

from __future__ import annotations

from rich.console import Console

import braintrust_migrate.cli as cli_module


def _results(*, skipped: int, report_path: str | None) -> dict:
    res: dict = {
        "summary": {
            "total_projects": 1,
            "total_resources": 3,
            "migrated_resources": 3 - skipped,
            "skipped_resources": skipped,
            "failed_resources": 0,
            "errors": [],
        },
        "projects": {},
    }
    if report_path is not None:
        res["report_path"] = report_path
    return res


def test_skip_pointer_shown_when_resources_skipped(monkeypatch):
    rec = Console(record=True, width=200)
    monkeypatch.setattr(cli_module, "console", rec)

    cli_module._display_results(
        _results(skipped=2, report_path="/tmp/checkpoints/migration_report.json")
    )

    out = rec.export_text()
    assert "2 resource(s) skipped" in out
    assert "/tmp/checkpoints/migration_report.json" in out
    assert "detailed_breakdown.skipped" in out


def test_no_skip_pointer_when_nothing_skipped(monkeypatch):
    rec = Console(record=True, width=200)
    monkeypatch.setattr(cli_module, "console", rec)

    cli_module._display_results(
        _results(skipped=0, report_path="/tmp/checkpoints/migration_report.json")
    )

    out = rec.export_text()
    assert "resource(s) skipped" not in out
    assert "detailed_breakdown.skipped" not in out
