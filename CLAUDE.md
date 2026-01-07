# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
uv run python -m honey_duck                      # Run default pipeline
uv run python -m honey_duck --dry-run            # Show plan only
uv run python -m honey_duck pipelines/full.py    # Run specific pipeline
uv run python -m honey_duck --db path/to/db      # Use file-based DuckDB
uv run pytest                                    # Run tests
```

## Structure

```
honey_duck/
├── base.py        # Processor ABCs (Processor, PolarsProcessor, DuckDBProcessor)
├── pipeline.py    # DuckDBPipeline class (harvest, run, output)
├── processors.py  # Built-in processor implementations
└── __main__.py    # CLI runner

pipelines/         # Pipeline definitions (SOURCES, PROCESSORS, OUTPUT)
data/
├── input/         # Source data (CSV files)
└── output/        # Generated output (JSON, DuckDB files)
```

## Key Patterns

**Pipeline definitions** are Python files with:
- `SOURCES` - dict of `{table_name: path}`
- `PROCESSORS` - list of processor instances
- `OUTPUT` - output file path
- `DB_PATH` (optional) - path to DuckDB file instead of in-memory

**Processor naming**: All processors are prefixed with their type:
- `DuckDB*` - SQL processors (DuckDBSQLProcessor, DuckDBLookupProcessor, DuckDBAggregateProcessor)
- `Pandas*` - pandas processors (PandasFilterProcessor, PandasUppercaseProcessor)
- `Polars*` - polars processors (PolarsAggregateProcessor, PolarsWindowProcessor)

**DuckDBProcessor** runs SQL directly without loading data into Python - use for joins/lookups.

**Harvest methods** accept optional `table_name` to load lookup tables alongside main data.
