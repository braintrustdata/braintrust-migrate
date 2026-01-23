# Braintrust Migration Tool

> **⚠️ WARNING: Large-scale migrations (especially logs/experiments) can be extremely expensive and operationally risky. This tool includes streaming + resumable migration for high-volume event streams, but TB-scale migrations have not been fully soak-tested in production-like conditions. Use with caution and test on a subset first.**

A Python CLI & library for migrating Braintrust organizations with maximum fidelity, leveraging the official `braintrust-api-py` SDK.

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

### Environment Variables

Create a `.env` file with your configuration:

```bash
# Copy the example file
cp .env.example .env
```

**Required Configuration:**
```bash
# Source organization (where you're migrating FROM)
BT_SOURCE_API_KEY=your_source_api_key_here
BT_SOURCE_URL=https://api.braintrust.dev

# Destination organization (where you're migrating TO)  
BT_DEST_API_KEY=your_destination_api_key_here
BT_DEST_URL=https://api.braintrust.dev
```

**Optional Configuration:**
```bash
# Logging
LOG_LEVEL=INFO                    # DEBUG, INFO, WARNING, ERROR
LOG_FORMAT=json                   # json, text

# Optional CLI defaults (can also be passed as flags)
# - MIGRATION_RESOURCES: comma-separated list (e.g. "logs,experiments")
# - MIGRATION_PROJECTS: comma-separated list of project names
MIGRATION_RESOURCES=all
MIGRATION_PROJECTS=

# Performance tuning
MIGRATION_BATCH_SIZE=100          # Resources per batch
MIGRATION_RETRY_ATTEMPTS=3        # Retry failed operations
MIGRATION_RETRY_DELAY=1.0         # Initial retry delay (seconds)
MIGRATION_MAX_CONCURRENT=10       # Concurrent operations
MIGRATION_CHECKPOINT_INTERVAL=50  # Checkpoint frequency

# Storage
MIGRATION_STATE_DIR=./checkpoints # Checkpoint directory

# (Advanced) Explicitly resume from a timestamped run directory.
# Prefer passing MIGRATION_STATE_DIR as the run directory instead.
MIGRATION_RESUME_RUN_DIR=

# Streaming migration tuning (high-volume resources: logs, experiments, datasets)
# All streaming resources use BTQL sorted pagination for scalability.
# Unified env vars apply to all; resource-specific vars override if set.
MIGRATION_EVENTS_FETCH_LIMIT=1000          # BTQL fetch page size (rows/spans)
MIGRATION_EVENTS_INSERT_BATCH_SIZE=200     # Insert batch size
MIGRATION_EVENTS_USE_SEEN_DB=true          # SQLite deduplication store

# Resource-specific overrides (optional - only if one resource needs different settings)
# MIGRATION_LOGS_FETCH_LIMIT=500           # Override just for logs
# MIGRATION_EXPERIMENT_EVENTS_INSERT_BATCH_SIZE=100  # Override just for experiments
# MIGRATION_DATASET_EVENTS_USE_SEEN_DB=false         # Override just for datasets

# Insert request sizing (for large events with attachments)
MIGRATION_INSERT_MAX_REQUEST_BYTES=6291456  # 6MB max request size (default)
MIGRATION_INSERT_REQUEST_HEADROOM_RATIO=0.75  # Use 75% of max → ~4.5MB effective limit

# Optional time-based filtering
MIGRATION_CREATED_AFTER=                           # Date filter (e.g. 2026-01-15)
                                                   # - Logs: only migrate events created >= this date
                                                   # - Experiments: only migrate experiments created >= this date
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
# Only migrate logs/experiments created on or after a certain date
braintrust-migrate migrate --created-after 2026-01-15

# Applies to:
# - Logs: filters individual events by created date
# - Experiments: filters which experiments to migrate (all their events are included)
```

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

The migration follows a dependency-aware order:

#### Organization-Scoped Resources (Migrated Once)
1. **AI Secrets** - AI provider credentials (OpenAI, Anthropic, etc.)
2. **Roles** - Organization-level role definitions  
3. **Groups** - Organization-level user groups

#### Project-Scoped Resources (Migrated Per Project)
4. **Datasets** - Training and evaluation data
5. **Project Tags** - Project-level metadata tags
6. **Span Iframes** - Custom span visualization components
7. **Functions** - Tools, scorers, tasks, and LLMs (migrated before prompts)
8. **Prompts** - Template definitions that can use functions as tools
9. **Project Scores** - Scoring configurations
10. **Experiments** - Experiment metadata + event streams (BTQL sorted pagination)
11. **Logs** - Project logs / traces (BTQL sorted pagination)
12. **Views** - Custom project views

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

# Migrate incrementally
braintrust-migrate migrate --resources ai_secrets,datasets
braintrust-migrate migrate --resources prompts,functions
```

**4. Network Issues**
- **Timeouts**: Increase retry attempts and delay
- **Rate Limits**: Reduce batch size and concurrent operations; the client respects `Retry-After` when throttled (429)
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
- **Not for Large-Scale Data**: This tool is not thoroughly tested for large-scale logs or experiments. Use for POC/test data only. 