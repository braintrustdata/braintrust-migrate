from __future__ import annotations

from pathlib import Path

from braintrust_migrate.cli import (
    _normalize_checkpoint_args,
    _resolve_run_checkpoint_dir,
)


def test_resolve_checkpoint_dir_uses_resume_run_dir(tmp_path: Path) -> None:
    root = tmp_path / "checkpoints"
    root.mkdir(parents=True, exist_ok=True)
    resume = root / "20260113_212530"
    resume.mkdir(parents=True, exist_ok=True)
    ckpt, is_resuming = _resolve_run_checkpoint_dir(
        state_dir=root,
        resume_run_dir=resume,
    )
    assert ckpt == resume
    assert is_resuming is True


def test_resolve_checkpoint_dir_creates_timestamp_subdir_when_not_resuming(
    tmp_path: Path,
) -> None:
    root = tmp_path / "checkpoints"
    root.mkdir(parents=True, exist_ok=True)
    ckpt, is_resuming = _resolve_run_checkpoint_dir(
        state_dir=root,
        resume_run_dir=None,
    )
    # Not deterministic, but should be a direct child of root.
    assert ckpt.parent == root
    assert is_resuming is False


def test_normalize_checkpoint_args_state_dir_can_be_run_dir(tmp_path: Path) -> None:
    root = tmp_path / "checkpoints"
    root.mkdir(parents=True, exist_ok=True)
    run_dir = root / "20260113_212530"
    run_dir.mkdir(parents=True, exist_ok=True)
    # Create a minimal marker indicating this is a run dir.
    (run_dir / "migration_report.json").write_text("{}")

    normalized_root, resume_run_dir, inferred_project = _normalize_checkpoint_args(
        state_dir_cli=run_dir,
        resume_run_dir_cli=None,
    )
    assert normalized_root == root
    assert resume_run_dir == run_dir
    assert inferred_project is None


def test_normalize_checkpoint_args_state_dir_can_be_project_dir(tmp_path: Path) -> None:
    root = tmp_path / "checkpoints"
    root.mkdir(parents=True, exist_ok=True)
    run_dir = root / "20260113_212530"
    project_dir = run_dir / "langgraph-supervisor"
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / "logs_streaming_state.json").write_text(
        '{"btql_min_pagination_key":"p1"}'
    )

    normalized_root, resume_run_dir, inferred_project = _normalize_checkpoint_args(
        state_dir_cli=project_dir,
        resume_run_dir_cli=None,
    )
    assert normalized_root == root
    assert resume_run_dir == run_dir
    assert inferred_project == "langgraph-supervisor"


def test_normalize_checkpoint_args_project_dir_without_state_files_does_not_nest(
    tmp_path: Path,
) -> None:
    # This reproduces the "weird nesting" behavior:
    # If a user points --state-dir at checkpoints/<run>/<project> and the directory is
    # empty (no state files yet), we still want to treat it as a project checkpoint dir
    # under that run, not as a new root directory.
    root = tmp_path / "checkpoints"
    root.mkdir(parents=True, exist_ok=True)
    run_dir = root / "20260113_212530"
    project_dir = run_dir / "langgraph-supervisor"
    project_dir.mkdir(parents=True, exist_ok=True)

    normalized_root, resume_run_dir, inferred_project = _normalize_checkpoint_args(
        state_dir_cli=project_dir,
        resume_run_dir_cli=None,
    )
    assert normalized_root == root
    assert resume_run_dir == run_dir
    assert inferred_project == "langgraph-supervisor"
