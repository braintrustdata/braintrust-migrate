# Braintrust Migration Tool

> **⚠️ WARNING: Large-scale migrations (especially logs/experiments) can be extremely expensive and operationally risky. This tool includes streaming + resumable migration for high-volume event streams, but TB-scale migrations have not been fully soak-tested in production-like conditions. Use with caution and test on a subset first.**

A Python CLI & library for migrating Braintrust organizations with maximum fidelity, using direct HTTP requests (via `httpx`) against the Braintrust REST API.

## Overview

This tool provides migration capabilities for Braintrust organizations, handling everything from AI provider credentials to project-level data. **It is best suited for small-scale migrations, such as moving POC/test data to a new deployment.**

- **Organization administrators** migrating between environments (dev → staging → prod)
- **Teams** consolidating multiple organizations
- **Enterprises** setting up new Braintrust instances
- **Developers** contributing to migration tooling

### Key Capabilities

- **Resource Coverage**: Migrates most Braintrust resources including AI secrets, datasets, prompts, functions, experiments, and more
- **Dependency Resolution**: Handles resource dependencies (e.g., functions referenced by prompts, datasets referenced by experiments)
- **Organization vs Project Scope**: Org-level resources are migrated once, project-level resources per project
- **Real-time Progress**: Live progress indicators and detailed migration reports
- **High-volume Streaming**: Logs, experiment events, and dataset events are migrated via BTQL sorted pagination (by `_pagination_key`) with bounded insert batches
- **Resume + Idempotency**: Per-resource/per-experiment checkpoints + a SQLite "seen ids" store enable safe resume and help avoid duplicate inserts/overwrites
- **Rate Limit Resilience**: Automatic LIMIT backoff on 500/504 errors (retries with progressively smaller page sizes: 1000 → 500 → 250 → ...)

## Features

### Migration Features
- **Dependency-Aware Migration**: Resources are migrated in an order that respects dependencies (see below)
- **Organization Scoping**: AI secrets, roles, and groups migrated once at org level
- **Batch Processing**: Configurable batch sizes for optimal performance
- **Multi-Level Parallelization**: Concurrent resource types, concurrent items within a type, and pipelined event streaming (see [Parallelization](#parallelization) below)

### Reliability Features
- **Retry Logic**: Adaptive retries with exponential backoff + jitter; respects `Retry-After` when rate-limited (429)
- **Validation**: Pre-flight connectivity and permission checks
- **Error Recovery**: Detailed error reporting with actionable guidance

### Observability Features
- **Real-time Progress**: Live updates on what's being created, skipped, or failed
- **Comprehensive Reporting**: JSON + human-readable migration summaries
- **Structured Logging**: JSON and text formats with configurable detail levels
- **Skip Analysis**: Detailed breakdowns of why resources were skipped

## Installation

### Prerequisites

- **Python 3.8+** (3.12+ recommended)
- **API Keys** for source and destination Braintrust organizations
- **Network Access** to Braintrust API endpoints

### Quick Start

```bash
# Clone the repository
git clone https://github.com/braintrustdata/braintrust-migrate
cd braintrust-migrate

# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install with uv (recommended)
uv sync --all-extras
source .venv/bin/activate

# Or install with pip
pip install -e .

# Verify installation
braintrust-migrate --help
```

### Development Setup

```bash
# Install development dependencies
uv sync --all-extras --dev

# Install pre-commit hooks
pre-commit install

# Run tests to verify setup
pytest
```

## Configuration

### Configuration Reference

All options can be set via environment variables or CLI flags. CLI flags take precedence over environment variables.

#### Required Settings

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `BT_SOURCE_API_KEY` | — | — | **Required.** API key for source organization |
| `BT_SOURCE_URL` | — | `https://api.braintrust.dev` | Source Braintrust API URL |
| `BT_DEST_API_KEY` | — | — | **Required.** API key for destination organization |
| `BT_DEST_URL` | — | `https://api.braintrust.dev` | Destination Braintrust API URL |

#### Migration Scope

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `MIGRATION_RESOURCES` | `--resources`, `-r` | `all` | Comma-separated list of resources to migrate. Options: `all`, `ai_secrets`, `roles`, `groups`, `datasets`, `project_tags`, `span_iframes`, `functions`, `prompts`, `project_scores`, `experiments`, `logs`, `views` |
| `MIGRATION_PROJECTS` | `--projects`, `-p` | *(all projects)* | Comma-separated list of project names to migrate |
| `MIGRATION_CREATED_AFTER` | `--created-after` | *(none)* | Only migrate data created on or after this date (**inclusive**: `>=`). Format: `YYYY-MM-DD` or ISO-8601 |
| `MIGRATION_CREATED_BEFORE` | `--created-before` | *(none)* | Only migrate data created before this date (**exclusive**: `<`). Format: `YYYY-MM-DD` or ISO-8601 |

#### Logging

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `LOG_LEVEL` | `--log-level`, `-l` | `INFO` | Log verbosity. Options: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `LOG_FORMAT` | `--log-format`, `-f` | `text` | Log output format. Options: `json`, `text` |

#### State & Checkpoints

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `MIGRATION_STATE_DIR` | `--state-dir`, `-s` | `./checkpoints` | Directory for migration state and checkpoints. Can be a root dir, run dir, or project dir (see Resume section) |

#### Performance Tuning

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `MIGRATION_BATCH_SIZE` | — | `100` | Number of resources to process per batch |
| `MIGRATION_RETRY_ATTEMPTS` | — | `3` | Number of retry attempts for failed operations (0 = no retries) |
| `MIGRATION_RETRY_DELAY` | — | `1.0` | Initial retry delay in seconds (exponential backoff) |
| `MIGRATION_MAX_CONCURRENT` | — | `10` | Maximum concurrent **projects** (bounded project-level parallelism) |
| `MIGRATION_CHECKPOINT_INTERVAL` | — | `50` | Write checkpoint every N successful operations |

#### Parallelization Tuning

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `MIGRATION_MAX_CONCURRENT_RESOURCES` | — | `5` | Max concurrent items within a resource type (e.g. 5 experiments migrating at once). Also controls concurrent event streams for datasets/experiments. Range: 1–50 |
| `MIGRATION_STREAMING_PIPELINE` | — | `true` | Prefetch the next BTQL page while inserting the current batch, overlapping source reads with destination writes |
| `MIGRATION_MAX_CONCURRENT_REQUESTS` | — | `20` | Global cap on concurrent HTTP requests per client (source and destination independently). Prevents API overwhelm when multiple parallelization layers are active. Range: 1–200 |

#### Streaming Migration (Logs, Experiments, Datasets)

These settings control BTQL-based streaming for high-volume resources.

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `MIGRATION_EVENTS_FETCH_LIMIT` | — | `1000` | BTQL fetch page size (rows per query) |
| `MIGRATION_EVENTS_INSERT_BATCH_SIZE` | — | `200` | Events per insert API call |
| `MIGRATION_EVENTS_USE_SEEN_DB` | — | `true` | Use SQLite store for deduplication |
| `MIGRATION_LOGS_FETCH_LIMIT` | `--logs-fetch-limit` | *(inherits)* | Override fetch limit for logs only |
| `MIGRATION_LOGS_INSERT_BATCH_SIZE` | `--logs-insert-batch-size` | *(inherits)* | Override insert batch size for logs only |`

Resource-specific overrides follow the pattern `MIGRATION_{RESOURCE}_FETCH_LIMIT`, `MIGRATION_{RESOURCE}_INSERT_BATCH_SIZE`, `MIGRATION_{RESOURCE}_USE_SEEN_DB` where `{RESOURCE}` is `LOGS`, `EXPERIMENT_EVENTS`, or `DATASET_EVENTS`.

#### Insert Request Sizing

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `MIGRATION_INSERT_MAX_REQUEST_BYTES` | — | `6291456` (6MB) | Maximum HTTP request payload size |
| `MIGRATION_INSERT_REQUEST_HEADROOM_RATIO` | — | `0.75` | Target ratio of max size (0.75 = ~4.5MB effective limit) |

#### Other Options

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| — | `--dry-run`, `-n` | `false` | Validate configuration without making changes |
| — | `--config`, `-c` | *(none)* | Path to YAML/JSON configuration file |
| `MIGRATION_COPY_ATTACHMENTS` | — | `false` | Copy Braintrust-managed attachments between orgs |
| `MIGRATION_ATTACHMENT_MAX_BYTES` | — | `52428800` (50MB) | Maximum attachment size to copy |

---

### Environment Variables

Create a `.env` file with your configuration:

```bash
# Copy the example file
cp .env.example .env
```

**Example `.env` file:**
```bash
# Required: API keys
BT_SOURCE_API_KEY=your_source_api_key_here
BT_DEST_API_KEY=your_destination_api_key_here

# Optional: Custom URLs (defaults to https://api.braintrust.dev)
# BT_SOURCE_URL=https://api.braintrust.dev
# BT_DEST_URL=https://api.braintrust.dev

# Optional: Logging
LOG_LEVEL=INFO
LOG_FORMAT=text

# Optional: Date filtering
# MIGRATION_CREATED_AFTER=2026-01-01
# MIGRATION_CREATED_BEFORE=2026-02-01
```

### Getting API Keys

1. **Log into Braintrust** → Go to your organization settings
2. **Navigate to API Keys** → Usually under Settings or Developer section
3. **Generate New Key** → Create with appropriate permissions:
   - **Source**: Read permissions for all resource types
   - **Destination**: Write permissions for resource creation
4. **Copy Keys** → Add to your `.env` file

**Permission Requirements:**
- Source org: `read:all` or specific resource read permissions
- Destination org: `write:all` or specific resource write permissions

## Usage

### Basic Commands

**Validate Configuration:**
```bash
# Test connectivity and permissions
braintrust-migrate validate
```

**Complete Migration:**
```bash
# Migrate all resources
braintrust-migrate migrate
```

**Selective Migration:**
```bash
# Migrate specific resource types
braintrust-migrate migrate --resources ai_secrets,datasets,prompts

# Migrate specific projects only
braintrust-migrate migrate --projects "Project A","Project B"
```

**Resume Migration:**
```bash
# Resume from last checkpoint (automatic)
#
# `--state-dir` (or MIGRATION_STATE_DIR) can be:
# - a root directory (creates a new timestamped run dir under it)
# - a run directory (resumes that run)
# - a project directory within a run (resumes that run and infers the project)
#
# Root checkpoints dir (new run):
braintrust-migrate migrate --state-dir ./checkpoints
#
# Resume from a specific run:
braintrust-migrate migrate --state-dir ./checkpoints/20260113_212530
#
# Resume just one project from a run:
braintrust-migrate migrate --state-dir ./checkpoints/20260113_212530/langgraph-supervisor --resources logs
```

### Advanced Usage

**Custom Configuration:**
```bash
braintrust-migrate migrate \
  --state-dir ./production-migration \
  --log-level DEBUG \
  --log-format text \
  --batch-size 50
```

**Dry Run (Validation Only):**
```bash
braintrust-migrate migrate --dry-run
```

**Time-based Filtering:**
```bash
# Migrate data from a specific date onward (inclusive: >=)
braintrust-migrate migrate --created-after 2026-01-15

# Migrate data before a specific date (exclusive: <)
braintrust-migrate migrate --created-before 2026-02-01

# Date range: migrate all of January 2026
braintrust-migrate migrate --created-after 2026-01-01 --created-before 2026-02-01
```

> **Semantics:** `--created-after` is inclusive (`>=`), `--created-before` is exclusive (`<`). This half-open interval `[after, before)` makes date ranges intuitive—e.g., "all of January" is `--created-after 2026-01-01 --created-before 2026-02-01`.

### CLI Reference

```bash
# General help
braintrust-migrate --help

# Command-specific help
braintrust-migrate migrate --help
braintrust-migrate validate --help
```

## Migration Process

### Resource Migration Order

The migration follows a dependency-aware order. Within each phase, **independent resource types run concurrently** (DAG-scheduled), and dependent types start as soon as their prerequisites finish.

#### Organization-Scoped Resources (Migrated Once, Concurrently)
- **AI Secrets** - AI provider credentials (OpenAI, Anthropic, etc.)
- **Roles** - Organization-level role definitions  
- **Groups** - Organization-level user groups

These three are independent and run in parallel.

#### Project-Scoped Resources (Migrated Per Project, DAG-Scheduled)

Resource types are arranged in a dependency graph. Types in the same tier run concurrently:

```
Tier 0 (no deps):      Datasets, Project Tags, Span Iframes, Logs, Project Automations
Tier 1 (needs Tier 0): Functions, Experiments
Tier 2 (needs Tier 1): Prompts, Project Scores
Tier 3 (needs above):  Views
```

For example: Datasets, Project Tags, Span Iframes, Logs, and Project Automations all start at the same time. Functions and Experiments start as soon as Datasets finishes. Prompts and Project Scores start as soon as Functions finishes. Views starts once both Datasets and Experiments are done.

### Smart Dependency Handling

- **Functions are migrated before prompts** to ensure all function references in prompts can be resolved.
- **Experiments** handle dependencies on datasets and other experiments (via `base_exp_id`) in a single pass with dependency-aware ordering.
- **ID mapping and dependency resolution** are used throughout to ensure references are updated to the new organization/project.
- **Prompts** are migrated in a single pass; prompt origins and tool/function references are remapped via ID mappings.
- **ACLs**: Support is present in the codebase but may be experimental or disabled by default.
- **Agents and users**: Not supported for migration (users are org-specific; agents are not present in the codebase).

### Progress Monitoring

**Real-time Updates:**
```
2024-01-15 10:30:45 [info] Starting organization-scoped resource migration
2024-01-15 10:30:46 [info] ✅ Created AI secret: 'OpenAI API Key' (src-123 → dest-456)
2024-01-15 10:30:47 [info] ⏭️  Skipped role: 'Admin' (already exists)
2024-01-15 10:30:48 [info] Starting project-scoped resource migration
2024-01-15 10:30:49 [info] ✅ Created dataset: 'Training Data' (src-789 → dest-012)
```

**Comprehensive Reporting:**
After migration, you'll get:
- **JSON Report** (`migration_report.json`) - Machine-readable detailed results
- **Human Summary** (`migration_summary.txt`) - Readable overview with skip analysis
- **Checkpoint Files** - Resume state for interrupted migrations

### Checkpointing & Resume

The tool uses **two-level checkpointing** for streaming resources (logs, experiments, datasets):

**Level 1: Resource Metadata**
- `{project}/experiments_state.json` - tracks which experiments are created
- Contains `completed_ids`, `failed_ids`, and `id_mapping` (source → dest)
- On resume: skips experiments in `completed_ids`

**Level 2: Event Streaming State (per-experiment/dataset)**
- `{project}/experiment_events/{exp_id}_state.json` - BTQL pagination position
- `{project}/experiment_events/{exp_id}_seen.sqlite3` - deduplication store
- Tracks `btql_min_pagination_key` (resume point) and counters (fetched/inserted)
- On resume: continues from last `_pagination_key`

**Example:** If you migrate 100 experiments and crash after:
- ✅ Metadata created for experiments 1-50
- ✅ All events migrated for experiments 1-30
- ⚠️  50% of events migrated for experiment 31 (crashed mid-stream)

On resume: skips 1-30 (done), resumes experiment 31 from saved `_pagination_key`, continues with 32-100.

## Parallelization

The migration tool uses **three levels of parallelization** that work together to reduce migration time. All levels are enabled by default and are controlled by the three env vars described in [Parallelization Tuning](#parallelization-tuning).

### How It Works

```
┌─────────────────────────────────────────────────────────────┐
│ Level 1: Inter-Project (MIGRATION_MAX_CONCURRENT=10)        │
│   Projects migrate concurrently (up to 10 at a time)        │
│                                                             │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Level 2: Inter-Resource-Type (DAG scheduler)            │ │
│ │   Within each project, independent resource types       │ │
│ │   (e.g. Datasets + Tags + Iframes) run concurrently.   │ │
│ │   Dependent types wait for prerequisites to finish.     │ │
│ │                                                         │ │
│ │ ┌─────────────────────────────────────────────────────┐ │ │
│ │ │ Level 3: Intra-Resource-Type                        │ │ │
│ │ │   Within each type, individual items migrate        │ │ │
│ │ │   concurrently (MIGRATION_MAX_CONCURRENT_RESOURCES) │ │ │
│ │ │                                                     │ │ │
│ │ │   For streaming resources (logs/datasets/exps):     │ │ │
│ │ │   - Multiple event streams run in parallel          │ │ │
│ │ │   - Each stream prefetches the next page while      │ │ │
│ │ │     inserting the current (STREAMING_PIPELINE)      │ │ │
│ │ └─────────────────────────────────────────────────────┘ │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Level Details

**Inter-Project Concurrency** (`MIGRATION_MAX_CONCURRENT`, default 10)
Multiple projects migrate at the same time. Each project runs its own DAG-scheduled resource pipeline independently. This was the only parallelism the tool had before the current changes.

**Inter-Resource-Type Concurrency** (automatic, DAG-based)
Within each project, resource types are organized in a dependency graph. Types whose dependencies are satisfied start immediately instead of waiting in a fixed sequence. For example, Datasets, Project Tags, and Span Iframes all start at the same time because they have no mutual dependencies.

**Intra-Resource-Type Concurrency** (`MIGRATION_MAX_CONCURRENT_RESOURCES`, default 5)
Within a single resource type, multiple individual items migrate concurrently. For example, if a project has 50 functions, up to 5 are created at once instead of one-by-one. For streaming resources (datasets, experiments), this also controls how many event streams run in parallel -- e.g. events for 5 datasets copy simultaneously.

**Pipelined Event Streaming** (`MIGRATION_STREAMING_PIPELINE`, default true)
For each individual event stream (logs, dataset records, experiment events), the next BTQL page is prefetched from the source while the current page's batches are being inserted into the destination. This overlaps source reads with destination writes, reducing idle time for large migrations.

### Safety Mechanisms

All parallelization levels are bounded by a **global HTTP request semaphore** (`MIGRATION_MAX_CONCURRENT_REQUESTS`, default 20) on each client. This prevents overwhelming the Braintrust API even when multiple parallelism layers are active simultaneously. The connection pool is automatically sized to match.

State mutations (ID mappings, checkpoint files) are protected by `asyncio.Lock` so concurrent tasks don't corrupt shared data.

### When to Tune

| Scenario | Recommended Settings |
|----------|---------------------|
| **Small migration** (<5 projects, <100 resources) | Defaults work well. No tuning needed. |
| **Many small projects** (50+ projects, small data) | Increase `MIGRATION_MAX_CONCURRENT=20` for more project-level parallelism. |
| **Few projects with many resources** (e.g. 500 experiments in one project) | Increase `MIGRATION_MAX_CONCURRENT_RESOURCES=10` for more intra-type parallelism. |
| **Large event streams** (TB-scale logs) | Defaults are good. Pipeline is on by default. Consider increasing `MIGRATION_MAX_CONCURRENT_REQUESTS=40` if the API can handle it. |
| **Rate-limited API** (frequent 429s) | *Decrease* `MIGRATION_MAX_CONCURRENT_RESOURCES=2` and `MIGRATION_MAX_CONCURRENT_REQUESTS=10`. The tool handles 429s with backoff, but fewer concurrent requests reduces throttling. |
| **Debugging or sequential run** | Set `MIGRATION_MAX_CONCURRENT_RESOURCES=1` and `MIGRATION_STREAMING_PIPELINE=false` for deterministic, sequential execution. |

### Example: Tuning for a Large Migration

```bash
# .env for a large migration with many projects and big event streams
BT_SOURCE_API_KEY=...
BT_DEST_API_KEY=...

# 15 projects at a time
MIGRATION_MAX_CONCURRENT=15

# 8 resources/event-streams per type
MIGRATION_MAX_CONCURRENT_RESOURCES=8

# Allow more HTTP connections (API can handle it)
MIGRATION_MAX_CONCURRENT_REQUESTS=40

# Pipeline is on by default, but explicit for clarity
MIGRATION_STREAMING_PIPELINE=true
```

```bash
# .env for a rate-limited or cautious migration
BT_SOURCE_API_KEY=...
BT_DEST_API_KEY=...

# Conservative parallelism
MIGRATION_MAX_CONCURRENT=5
MIGRATION_MAX_CONCURRENT_RESOURCES=2
MIGRATION_MAX_CONCURRENT_REQUESTS=10

# Disable pipeline for simpler debugging
MIGRATION_STREAMING_PIPELINE=false
```

## Resource Types

The following resource types are supported:

- **AI Secrets** (organization-scoped)
- **Roles** (organization-scoped)
- **Groups** (organization-scoped)
- **Datasets**
- **Project Tags**
- **Span Iframes**
- **Functions**
- **Prompts**
- **Project Scores**
- **Experiments**
- **Logs**
- **Views**
- **ACLs** (experimental; may be disabled)

> **Note:** Agents and users are not supported for migration.

## Troubleshooting

### Common Issues

**1. Authentication Errors**
```bash
# Verify API keys
braintrust-migrate validate

# Check key permissions
curl -H "Authorization: Bearer $BT_SOURCE_API_KEY" \
     https://api.braintrust.dev/v1/organization
```

**2. Dependency Errors**
- **Circular Dependencies**: If you hit a dependency loop, try migrating the involved resource types separately (or re-run; idempotent resources will skip)
- **Missing Resources**: Check source organization for required dependencies
- **Permission Issues**: Ensure API keys have read/write access

**3. Performance Issues**
```bash
# Reduce batch size
export MIGRATION_BATCH_SIZE=25

# Increase retry delay
export MIGRATION_RETRY_DELAY=2.0

# Reduce parallelism if hitting rate limits
export MIGRATION_MAX_CONCURRENT_RESOURCES=2
export MIGRATION_MAX_CONCURRENT_REQUESTS=10

# Disable pipelining for simpler debugging
export MIGRATION_STREAMING_PIPELINE=false

# Migrate incrementally
braintrust-migrate migrate --resources ai_secrets,datasets
braintrust-migrate migrate --resources prompts,functions
```

**4. Network Issues**
- **Timeouts**: Increase retry attempts and delay
- **Rate Limits**: Reduce `MIGRATION_MAX_CONCURRENT_RESOURCES` and `MIGRATION_MAX_CONCURRENT_REQUESTS`; the client respects `Retry-After` when throttled (429)
- **Connectivity**: Verify firewall and proxy settings

> **Tip:** If you want rate-limit retries/backoff to actually happen, ensure `MIGRATION_RETRY_ATTEMPTS` is **greater than 0**. If it is set to `0`, the tool will fail fast on 429/5xx without retrying.

### Debug Mode

Enable detailed logging for troubleshooting:

```bash
# Maximum verbosity
braintrust-migrate migrate \
  --log-level DEBUG \
  --log-format text

# Focus on specific issues
export LOG_LEVEL=DEBUG
braintrust-migrate validate
```

### Recovery Strategies

**Resume Interrupted Migration:**
```bash
# Automatic resume (recommended)
braintrust-migrate migrate

# Manual checkpoint specification (run directory)
braintrust-migrate migrate --state-dir ./checkpoints/20240115_103045

# Or point directly at a single project's checkpoint directory
braintrust-migrate migrate --state-dir ./checkpoints/20240115_103045/ProjectA --resources logs
```

**Partial Re-migration:**
```bash
# Re-migrate specific resource types
braintrust-migrate migrate --resources experiments,logs

# Re-migrate specific projects
braintrust-migrate migrate --projects "Failed Project"
```

## Project Structure

```
braintrust_migrate/
├── __init__.py                   # Package initialization
├── config.py                     # Configuration models (Pydantic)
├── client.py                     # Braintrust API client wrapper
├── orchestration.py              # Migration orchestrator & reporting
├── cli.py                        # Command-line interface (Typer)
├── resources/                    # Resource-specific migrators
│   ├── __init__.py
│   ├── base.py                   # Abstract base migrator class
│   ├── ai_secrets.py             # AI provider credentials
│   ├── datasets.py               # Training/evaluation data
│   ├── prompts.py                # Prompt templates
│   ├── functions.py              # Tools, scorers, tasks
│   ├── experiments.py            # Evaluation runs
│   ├── logs.py                   # Execution traces
│   ├── roles.py                  # Organization roles
│   ├── groups.py                 # Organization groups
│   └── views.py                  # Project views
└── checkpoints/                  # Migration state (created at runtime)
    ├── organization/             # Org-scoped resource checkpoints
    └── project_name/            # Project-scoped checkpoints

tests/
├── unit/                         # Unit tests (fast)
├── integration/                  # Integration tests (API mocking)
└── e2e/                         # End-to-end tests (real API)
```

## Development

### Contributing

We welcome contributions! Here's how to get started:

**1. Setup Development Environment:**
```bash
# Fork and clone the repository
git clone https://github.com/yourusername/migration-tool.git
cd migration-tool

# Install development dependencies
uv sync --all-extras --dev

# Install pre-commit hooks
pre-commit install
```

**2. Development Workflow:**
```bash
# Create feature branch
git checkout -b feature/your-feature-name

# Make changes and test
pytest                           # Run tests
ruff check --fix                # Lint and format
mypy braintrust_migrate         # Type checking

# Commit with pre-commit hooks
git commit -m "feat: add your feature"
```

**3. Testing:**
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=braintrust_migrate --cov-report=html

# Run specific test categories
pytest tests/unit/              # Fast unit tests
pytest tests/integration/       # Integration tests
pytest tests/e2e/              # End-to-end tests
```

### Code Quality Standards

- **Type Hints**: All functions must have type annotations
- **Documentation**: Docstrings for public APIs
- **Testing**: New features require tests
- **Linting**: Code must pass `ruff` checks
- **Formatting**: Automatic formatting with `ruff format`

### Adding New Resource Types

To add support for a new Braintrust resource type:

1. **Create Migrator Class** in `braintrust_migrate/resources/new_resource.py`
2. **Extend Base Class** from `ResourceMigrator[ResourceType]`
3. **Implement Required Methods**: `list_source_resources`, `migrate_resource`, etc.
4. **Add to Orchestration** in appropriate scope (organization vs project)
5. **Write Tests** covering the new functionality
6. **Update Documentation** including this README

## Migration Examples

### Example 1: Development to Production

```bash
# Setup environment for dev → prod migration
cat > .env << EOF
BT_SOURCE_API_KEY="dev_org_api_key_here"
BT_SOURCE_URL="https://api.braintrust.dev"
BT_DEST_API_KEY="prod_org_api_key_here"  
BT_DEST_URL="https://api.braintrust.dev"
LOG_LEVEL=INFO
EOF

# Validate before migrating
braintrust-migrate validate

# Run complete migration
braintrust-migrate migrate
```

### Example 2: Incremental Migration

```bash
# Phase 1: Setup and data
braintrust-migrate migrate --resources ai_secrets,datasets

# Phase 2: Logic and templates  
braintrust-migrate migrate --resources prompts,functions

# Phase 3: Experiments and results
braintrust-migrate migrate --resources experiments,logs
```

### Example 3: Specific Project Migration

```bash
# Migrate only specific projects
braintrust-migrate migrate --projects "Customer Analytics","Model Evaluation"

# Later migrate remaining projects
braintrust-migrate migrate
```

### Example 4: Resume After Failure

```bash
# If migration fails partway through:
braintrust-migrate migrate
# Automatically resumes from last checkpoint

# Or specify checkpoint directory:
braintrust-migrate migrate --state-dir ./checkpoints/20240115_103045
```

## API Documentation

### Braintrust API Resources
- [Braintrust API Reference](https://www.braintrust.dev/docs/reference/api)
- [Python SDK Documentation](https://github.com/braintrustdata/braintrust-api-py)
- [AI Secrets API](https://www.braintrust.dev/docs/reference/api/AiSecrets)

### Migration Tool APIs
- **Config Models**: See `braintrust_migrate/config.py` for configuration options
- **Resource Migrators**: Base classes in `braintrust_migrate/resources/base.py`
- **Client Wrapper**: API helpers in `braintrust_migrate/client.py`

## Support

### Getting Help

1. **Check Documentation**: Start with this README and inline code documentation
2. **Review Logs**: Enable debug logging for detailed troubleshooting information
3. **Validate Setup**: Use `braintrust-migrate validate` to test configuration
4. **Check Issues**: Search existing GitHub issues for similar problems
5. **Create Issue**: Open a new issue with detailed information including:
   - Error messages and logs
   - Configuration (sanitized)
   - Migration command used
   - Environment details

### Best Practices

**Before Migration:**
- Test with a small subset of data first
- Backup critical data in source organization
- Verify API key permissions
- Plan for AI secret reconfiguration

**During Migration:**
- Monitor progress through logs
- Don't interrupt during critical operations
- Keep network connection stable

**After Migration:**
- Verify migrated data completeness
- Reconfigure AI provider credentials
- Test functionality in destination organization
- Archive migration reports for compliance

## License

This project is licensed under the MIT License. See the LICENSE file for details.

---

## Quick Reference

### Essential Commands
```bash
braintrust-migrate validate                    # Test setup
braintrust-migrate migrate                     # Full migration
braintrust-migrate migrate --dry-run           # Validation only
braintrust-migrate migrate --resources ai_secrets,datasets  # Selective migration
```

### Key Files
- `.env` - Configuration
- `checkpoints/` - Migration state
- `migration_report.json` - Detailed results
- `migration_summary.txt` - Human-readable summary

### Important Notes
- **AI Secrets**: Only metadata migrated; manually configure actual API keys
- **Dependency Order**: Functions are migrated before prompts; all dependencies are resolved via ID mapping
- **Organization Scope**: Some resources migrated once, others per project
- **Resume Capability**: Interrupted migrations automatically resume from checkpoints
- **Parallelization**: Resource types, individual resources, and event streams run concurrently by default. See the [Parallelization](#parallelization) section for tuning.
- **Not for Large-Scale Data**: This tool is not thoroughly tested for large-scale logs or experiments. Use for POC/test data only. 