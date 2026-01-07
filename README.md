# honey-duck

A DuckDB-backed DataFrame pipeline with Dagster orchestration, demonstrating the lakehouse pattern with dlt harvest and processor utilities.

## Quick Start

```bash
# Install dependencies
uv sync

# Set Dagster home for persistence
export DAGSTER_HOME=$(pwd)/dagster_home

# Start Dagster UI
uv run dagster dev

# Or run pipeline via CLI
uv run dagster job execute -j full_pipeline
```

## Architecture

```
CSV files → dlt harvest → Transform asset → Output assets → JSON
                 ↓              ↓                 ↓
             DuckDB raw    DuckDB main    DuckDB main + JSON files
             schema        schema         (via IOManager)
```

Uses [dlt](https://dlthub.com/) (data load tool) for CSV harvesting with automatic schema inference, and [dagster-dlt](https://docs.dagster.io/integrations/dlt) for Dagster integration.

## Asset Graph

```
dlt_honey_duck_harvest_sales_raw ────────┐
                                         │
dlt_honey_duck_harvest_artworks_raw ─────┼──→ sales_enriched ──┬──→ sales_output
                                         │                     │
dlt_honey_duck_harvest_artists_raw ──────┘                     └──→ artworks_output
```

**Groups:**
- `harvest` - Raw data loaded from CSV into DuckDB via dlt
- `transform` - Joined and enriched data
- `output` - Final outputs (sales + artworks JSON)

**Jobs:**
- `full_pipeline` - All assets (harvest → transform → output)

## Project Structure

```
honey_duck/
  __init__.py        # Package metadata
  defs/
    __init__.py      # Re-exports defs
    definitions.py   # Combined Dagster Definitions
    dlt_sources.py   # dlt source configuration for CSV files
    dlt_assets.py    # dagster-dlt asset wrapper
    assets.py        # Transform and output assets
    resources.py     # Path constants and configuration
    jobs.py          # Job definitions
    checks.py        # Asset checks

cogapp_deps/         # Processor utilities (simulates external package)
  processors/
    __init__.py      # Chain class for composing processors
    pandas/          # PandasReplaceOnConditionProcessor
    polars/          # PolarsFilterProcessor, PolarsStringProcessor
    duckdb/          # DuckDBJoinProcessor, DuckDBWindowProcessor, DuckDBAggregateProcessor

data/
  input/             # Source CSV files (Wyeth auction data)
  output/            # Generated output (dagster.duckdb, *.json)

dagster_home/        # Dagster persistence (run history, schedules, etc.)
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DAGSTER_HOME` | Dagster persistence directory | (required) |
| `HONEY_DUCK_DB_PATH` | DuckDB database path | `data/output/dagster.duckdb` |
| `HONEY_DUCK_SALES_OUTPUT` | Sales JSON output path | `data/output/sales_output.json` |
| `HONEY_DUCK_ARTWORKS_OUTPUT` | Artworks JSON output path | `data/output/artworks_output.json` |

## DuckDB Lakehouse Benefits

1. **Persistence** - Data survives between runs (vs in-memory loss)
2. **Queryable** - Inspect intermediate tables via DuckDB CLI
3. **Scalable** - Handles larger datasets without memory pressure
4. **Zero-copy** - Efficient pandas/polars integration
5. **Debuggable** - Each stage persisted, restart from any point

## Data Flow

1. **Harvest (dlt)** - Load CSV files into DuckDB `raw` schema with automatic schema inference
2. **Transform** - SQL joins + window aggregations via DuckDB processors, string transforms via Polars Chain
3. **Output** - Two views: high-value sales ($30M+) and artwork catalog with sales history

## Processors

Generic processor utilities in `cogapp_deps/processors/`:

- **DuckDBJoinProcessor** - Generate SQL for chained joins
- **DuckDBWindowProcessor** - Generate window function expressions
- **DuckDBAggregateProcessor** - Generate GROUP BY aggregations
- **PolarsFilterProcessor** - Filter rows by condition (with lazy evaluation)
- **PolarsStringProcessor** - String transformations (strip, upper, lower)
- **Chain** - Compose multiple processors with Polars lazy optimization
