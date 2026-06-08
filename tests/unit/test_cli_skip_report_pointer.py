"""The console summary always points to the JSON report to drill into detail."""

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


def _render(monkeypatch, results: dict) -> str:
    rec = Console(record=True, width=200)
    monkeypatch.setattr(cli_module, "console", rec)
    cli_module._display_results(results)
    return rec.export_text()


def test_report_path_shown_when_resources_skipped(monkeypatch):
    out = _render(
        monkeypatch,
        _results(skipped=2, report_path="/tmp/checkpoints/migration_report.json"),
    )
    assert "/tmp/checkpoints/migration_report.json" in out
    assert "Full per-item report" in out


def test_report_path_shown_even_when_nothing_skipped(monkeypatch):
    # The report pointer is general drill-in info, not gated on skips.
    out = _render(
        monkeypatch,
        _results(skipped=0, report_path="/tmp/checkpoints/migration_report.json"),
    )
    assert "/tmp/checkpoints/migration_report.json" in out
    assert "Full per-item report" in out


def test_no_report_line_when_report_path_absent(monkeypatch):
    out = _render(monkeypatch, _results(skipped=1, report_path=None))
    assert "Full per-item report" not in out
