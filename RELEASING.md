# Releasing `braintrust-migrate`

This repository uses a tag-based release flow so users can pin stable versions.

## Goals

- Keep `main` as integration branch.
- Publish immutable, versioned artifacts.
- Let users pin exact versions (`==X.Y.Z`) or exact tags (`@vX.Y.Z`).

## Branch Strategy

- `main`: ongoing development (not guaranteed stable at every commit).
- Feature/fix branches: all work lands via PR.
- Optional release hardening branch: `release/X.Y` when needed for stabilization.

## Versioning

Use Semantic Versioning:

- `MAJOR`: breaking behavior/API changes
- `MINOR`: backward-compatible features
- `PATCH`: backward-compatible bug fixes

Version source of truth is `pyproject.toml` (`[project].version`).

## Release Process

1. Ensure intended changes are merged to `main`.
2. Update `CHANGELOG.md`:
   - Move items from `Unreleased` into a new `X.Y.Z` section.
3. Bump `pyproject.toml` version to `X.Y.Z`.
4. Merge the release prep PR into `main`.
5. Create and push an annotated tag from `main`:

```bash
git checkout main
git pull --ff-only
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin vX.Y.Z
```

6. GitHub Actions will:
   - run tests
   - verify tag `vX.Y.Z` matches `pyproject.toml` version `X.Y.Z`
   - build package artifacts
   - publish to PyPI
   - create a GitHub Release

## Hotfixes

1. Branch from latest release tag: `hotfix/X.Y.Z+1`
2. Implement fix + tests.
3. Merge via PR to `main`.
4. Bump patch version and tag `vX.Y.Z+1`.

## Required PR Checks

- CI green (`Tests` workflow).
- Changelog updated for user-facing changes.
- Version bump included when preparing a release.

## Pinning for Users

PyPI pin:

```bash
pip install braintrust-migrate==X.Y.Z
```

Git tag pin:

```bash
pip install "git+https://github.com/braintrustdata/braintrust-migrate.git@vX.Y.Z"
```
