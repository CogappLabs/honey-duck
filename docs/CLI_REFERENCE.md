# Dagster CLI Reference - Quick Guide

Essential Dagster CLI commands for honey-duck development.

## Starting Dagster

### Development Server

```bash
# Start Dagster UI (recommended for development)
uv run dagster dev

# Start on specific port
uv run dagster dev --port 3001

# Start with specific code location
uv run dagster dev -f honey_duck/defs/definitions.py
```

**Opens**: http://localhost:3000

**What it does**:
- Starts web UI for visualization
- Auto-reloads when code changes
- Shows real-time logs
- Enables interactive materialization

### Production Server

```bash
# Start Dagster daemon (for schedules/sensors)
uv run dagster-daemon run

# Start webserver separately
uv run dagster-webserver -p 3000
```

## Asset Operations

### Materialization

```bash
# Materialize single asset
uv run dagster asset materialize -a sales_transform

# Materialize multiple assets
uv run dagster asset materialize -a sales_transform -a artworks_transform

# Materialize asset and all upstream dependencies
uv run dagster asset materialize -a sales_output --select +sales_output

# Materialize asset and all downstream dependencies
uv run dagster asset materialize -a sales_transform --select sales_transform+

# Materialize full lineage (upstream + asset + downstream)
uv run dagster asset materialize -a sales_transform --select +sales_transform+
```

### Listing Assets

```bash
# List all assets
uv run dagster asset list

# List assets in specific group
uv run dagster asset list --group transform_polars

# Show asset details
uv run dagster asset materialize -a sales_transform --explain
```

### Asset Wipe (Delete Materializations)

```bash
# Wipe specific asset
uv run dagster asset wipe -a sales_transform

# Wipe multiple assets
uv run dagster asset wipe -a sales_transform -a artworks_transform

# Wipe all assets in group
uv run dagster asset wipe --all --group transform_polars
```

**⚠️ Warning**: This deletes materialization history and cached data!

## Job Execution

### Running Jobs

```bash
# Execute complete job
uv run dagster job execute -j polars_pipeline

# Execute with config
uv run dagster job execute -j polars_pipeline --config config.yaml

# List all jobs
uv run dagster job list

# Show job details
uv run dagster job print -j polars_pipeline
```

### Available Jobs in honey-duck

```bash
# Original implementation
uv run dagster job execute -j full_pipeline

# Polars implementation (split assets)
uv run dagster job execute -j polars_pipeline

# Polars ops implementation (graph-backed)
uv run dagster job execute -j polars_ops_pipeline

# DuckDB SQL implementation
uv run dagster job execute -j duckdb_pipeline

# Polars FilesystemIOManager implementation
uv run dagster job execute -j polars_fs_pipeline

# Polars multi-asset implementation
uv run dagster job execute -j polars_multi_pipeline
```

## Code Locations

### Reloading Code

```bash
# Reload code location (when dev server running)
# → Just save your file, auto-reload happens

# Or manually reload
uv run dagster code-location reload
```

### Code Location Info

```bash
# List code locations
uv run dagster code-location list

# Show code location details
uv run dagster code-location info
```

## Schedules & Sensors

```bash
# List all schedules
uv run dagster schedule list

# Start a schedule
uv run dagster schedule start daily_pipeline

# Stop a schedule
uv run dagster schedule stop daily_pipeline

# List sensors
uv run dagster sensor list

# Start sensor
uv run dagster sensor start file_sensor

# Stop sensor
uv run dagster sensor stop file_sensor
```

## Run Operations

### Listing Runs

```bash
# List recent runs
uv run dagster run list

# List runs for specific job
uv run dagster run list -j polars_pipeline

# Show run details
uv run dagster run show <run_id>
```

### Run Management

```bash
# Delete specific run
uv run dagster run delete <run_id>

# Delete all runs for a job
uv run dagster run delete --all -j polars_pipeline

# Cancel running job
uv run dagster run cancel <run_id>
```

## Debugging & Inspection

### Logs

```bash
# View logs for a run
uv run dagster run logs <run_id>

# Follow logs in real-time
uv run dagster run logs <run_id> --follow
```

### Asset Checks

```bash
# Execute asset checks
uv run dagster asset check -a expensive_artworks

# Execute all checks for asset
uv run dagster asset check --select expensive_artworks+
```

## Advanced Selection Syntax

### Selection Patterns

```bash
# Select by prefix/suffix
uv run dagster asset materialize --select "sales_*"  # All starting with sales_
uv run dagster asset materialize --select "*_output" # All ending with _output

# Select by group
uv run dagster asset materialize --select "group:transform_polars"

# Select by tag
uv run dagster asset materialize --select "tag:critical"

# Combine selections
uv run dagster asset materialize --select "+sales_output,+artworks_output"
```

### Graph Traversal

```bash
# + means "include upstream"
--select +asset_name     # Asset + all upstream dependencies

# + means "include downstream"
--select asset_name+     # Asset + all downstream dependencies

# Both
--select +asset_name+    # Asset + full lineage

# Multiple hops
--select asset_name++    # Asset + 2 levels downstream
--select ++asset_name    # Asset + 2 levels upstream

# Depth limit
--select asset_name+5    # Asset + 5 levels downstream
```

## Environment Variables

```bash
# Set Dagster home directory
export DAGSTER_HOME=/path/to/dagster_home

# Set log level
export DAGSTER_CLI_LOG_LEVEL=DEBUG

# Disable colored output
export NO_COLOR=1

# Set database path (honey-duck specific)
export HONEY_DUCK_DB_PATH=/path/to/dagster.duckdb
```

## Configuration Files

### Project Configuration

```bash
# Validate dagster.yaml
uv run dagster instance info

# Show instance config
cat $DAGSTER_HOME/dagster.yaml
```

### Run Configuration

Create `config.yaml`:
```yaml
ops:
  my_asset:
    config:
      threshold: 1000
      include_nulls: false
```

Use it:
```bash
uv run dagster job execute -j my_job --config config.yaml
```

## Testing

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_integration.py

# Run with verbose output
uv run pytest -xvs

# Run tests matching pattern
uv run pytest -k "test_sales"

# Run tests with coverage
uv run pytest --cov=honey_duck --cov-report=html
```

## Utility Commands

### Instance Management

```bash
# Show instance info
uv run dagster instance info

# Migrate instance schema
uv run dagster instance migrate

# Concurrency limits
uv run dagster instance concurrency info
```

### Health Check

```bash
# Check if Dagster is running
curl http://localhost:3000/server_info

# Check code location health
uv run dagster code-location list
```

## Common Workflows

### Development Workflow

```bash
# 1. Start dev server
uv run dagster dev

# 2. Make code changes
# ... edit files ...

# 3. Auto-reload happens
# → Check browser for reload notification

# 4. Test in UI
# → Click asset → Materialize

# 5. Run tests
uv run pytest
```

### CI/CD Workflow

```bash
# 1. Install dependencies
uv sync

# 2. Run tests
uv run pytest

# 3. Execute pipeline
uv run dagster job execute -j polars_pipeline

# 4. Check exit code
echo $?  # 0 = success, non-zero = failure
```

### Debugging Workflow

```bash
# 1. Check recent runs
uv run dagster run list

# 2. View logs for failed run
uv run dagster run logs <run_id>

# 3. Materialize with verbose logging
DAGSTER_CLI_LOG_LEVEL=DEBUG uv run dagster asset materialize -a my_asset

# 4. Check asset data
ls -la data/output/storage/my_asset/
```

## Keyboard Shortcuts (UI)

When Dagster UI is open:

- `?` - Show help
- `g` then `a` - Go to Assets
- `g` then `j` - Go to Jobs
- `g` then `r` - Go to Runs
- `/` - Search

## Tips & Tricks

### 1. Dry Run (Explain Plan)

```bash
# See what would run without actually running
uv run dagster asset materialize -a sales_output --explain
```

### 2. Force Re-materialization

```bash
# Even if asset is up-to-date
uv run dagster asset materialize -a sales_transform --force
```

### 3. Asset Subset Materialization

```bash
# Only materialize specific group
uv run dagster asset materialize --select "group:output_polars"
```

### 4. Quick Asset Test

```bash
# Materialize single asset to test changes
uv run dagster asset materialize -a my_new_asset

# Check the output file
cat data/output/json/my_output.json
```

### 5. Pipeline Comparison

```bash
# Run different implementations side-by-side
uv run dagster job execute -j polars_pipeline &
uv run dagster job execute -j duckdb_pipeline &
wait
```

## Troubleshooting Commands

```bash
# Clear Dagster cache
rm -rf $DAGSTER_HOME/storage/*

# Reset instance
uv run dagster instance info
uv run dagster instance migrate

# Check Python environment
uv run python --version
uv run which python

# Validate code loads
uv run python -c "from honey_duck.defs.definitions import defs; print(defs)"

# Check for import errors
uv run dagster definitions validate
```

## Quick Reference Card

```bash
# 🚀 MOST COMMON COMMANDS

# Start UI
uv run dagster dev

# Materialize asset
uv run dagster asset materialize -a ASSET_NAME

# Run complete pipeline
uv run dagster job execute -j JOB_NAME

# Run tests
uv run pytest

# List assets
uv run dagster asset list

# View logs
uv run dagster run logs RUN_ID
```

---

**Pro Tip**: Bookmark http://localhost:3000 and use the UI for 90% of operations. The CLI is great for automation and CI/CD!
