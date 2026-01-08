# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync

# Optional: Enable persistent run history (copy .env.example to .env)
cp .env.example .env

# Start Dagster UI
uv run dagster dev

# Run pipeline via CLI
uv run dagster job execute -j full_pipeline

# Run tests
uv run pytest
```

## Structure

```
honey_duck/
├── __init__.py        # Package metadata
└── defs/
    ├── __init__.py    # Re-exports defs
    ├── definitions.py # Combined Dagster Definitions
    ├── dlt_sources.py # dlt source configuration for CSV files
    ├── dlt_assets.py  # dagster-dlt asset wrapper
    ├── assets.py      # Transform and output assets
    ├── resources.py   # Path constants and configuration
    ├── jobs.py        # Job definitions
    └── checks.py      # Asset checks

cogapp_deps/           # Processor utilities (simulates external package)
└── processors/
    ├── __init__.py    # Chain class for composing processors
    ├── pandas/        # PandasReplaceOnConditionProcessor
    ├── polars/        # PolarsFilterProcessor, PolarsStringProcessor
    └── duckdb/        # DuckDBJoinProcessor, DuckDBWindowProcessor, DuckDBAggregateProcessor

data/
├── input/             # Source CSV files (Wyeth auction data)
└── output/            # Generated output (dagster.duckdb, *.json)

dagster_home/          # Dagster persistence directory
```

## Key Patterns

**Asset graph**:
```
dlt_honey_duck_harvest_*_raw → sales_enriched ──┬──→ sales_output
                                                └──→ artworks_output
```

**Groups**:
- `harvest` - Raw data loaded from CSV into DuckDB via dlt
- `transform` - Joined and enriched data
- `output` - Final outputs (sales + artworks JSON)

**DuckDBPandasIOManager**: All assets returning pd.DataFrame are automatically persisted to DuckDB tables in `main` schema.

**dlt harvest**: CSV files loaded to DuckDB `raw` schema via dagster-dlt integration.

**Processors**: Generic utilities in `cogapp_deps/processors/` for SQL generation, filtering, string transforms.

**Chain**: Compose multiple PolarsStringProcessors with lazy evaluation optimization.

## Adding New Assets

1. Add asset function in `defs/assets.py`
2. Decorate with `@dg.asset(kinds={"duckdb"}, group_name="...")`
3. Add rich metadata via `context.add_output_metadata()`
4. Add to `definitions.py` assets list

## Adding New dlt Sources

1. Add resource to `dlt_sources.py` honey_duck_source function
2. Asset key will be auto-generated as `dlt_honey_duck_harvest_{name}`
3. Table created in `raw` schema
