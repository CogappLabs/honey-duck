# honey-duck

A DuckDB-backed DataFrame pipeline with Dagster orchestration, demonstrating the lakehouse pattern with dlt harvest and processor utilities.

## Quick Start

```bash
# Install dependencies
uv sync

# Optional: Enable persistent run history
cp .env.example .env
# Edit .env with your absolute path

# Start Dagster UI
uv run dagster dev

# Or run pipeline via CLI
uv run dagster job execute -j processors_pipeline
```

Open http://localhost:3000 and click **"Materialize all"** to run the pipeline!

### Frontend Dashboard

A Next.js dashboard for client-friendly pipeline monitoring:

```bash
# Start Dagster on port 3003
uv run dagster dev -p 3003

# In another terminal
cd frontend && npm install && npm run dev
```

Open http://localhost:3001 for the dashboard. See **[Frontend Dashboard Guide](docs/user-guide/frontend-dashboard.md)** for details.

## Documentation

### 🚀 New Users Start Here

**Zero to Hero**:
1. **[Quick Start Tutorial](docs/QUICK_START_TUTORIAL.md)** ⭐ - Build your first asset in 15 minutes
2. **[Getting Started Guide](docs/GETTING_STARTED.md)** - In-depth Dagster introduction
3. **[PATTERNS.md](honey_duck/defs/PATTERNS.md)** - Copy-paste asset patterns
4. **[Cogapp Deps API](cogapp_deps/README.md)** - Complete utility reference

### 📖 Core Documentation

**Essential Reading**:
- **[CLAUDE.md](CLAUDE.md)** - Project structure and commands
- **[Best Practices](docs/BEST_PRACTICES.md)** - Production guidelines
- **[Performance Tuning](docs/PERFORMANCE_TUNING.md)** ⚡ - Optimize your pipelines
- **[CLI Reference](docs/CLI_REFERENCE.md)** - Dagster commands
- **[Troubleshooting](docs/TROUBLESHOOTING.md)** - Common issues and solutions

### 🔌 Integrations

**Output Destinations**:
- **[Elasticsearch Integration](docs/ELASTICSEARCH_INTEGRATION.md)** - Full-text search with ES 8/9
- **[Common Outputs](docs/COMMON_OUTPUTS.md)** - PostgreSQL, BigQuery, S3, and more

**AI & Web**:
- **[API Bulk Harvesting](docs/API_BULK_HARVESTING.md)** - Claude & Voyage AI batch processing
- **[Sitemap Generation](docs/SITEMAP_GENERATION.md)** - SEO-optimized XML sitemaps
- **[IIIF Tile Generation](docs/IIIF_TILE_GENERATION.md)** - Image tile processing

**Development & Deployment**:
- **[LocalStack S3 Guide](docs/LOCALSTACK_S3_GUIDE.md)** - Local S3 testing
- **[Production Deployment](docs/DEPLOYMENT.md)** 🐳 - Docker deployment guide

## Architecture

```
CSV files → dlt harvest → Transform asset → Output assets → JSON
                 ↓              ↓                 ↓
             DuckDB raw    DuckDB main    DuckDB main + JSON files
             schema        schema         (via IOManager)
```

Uses [dlt](https://dlthub.com/) (data load tool) for CSV harvesting with automatic schema inference, and [dagster-dlt](https://docs.dagster.io/integrations/dlt) for Dagster integration.

## Asset Graph

6 parallel implementations sharing the harvest layer:

```
dlt_harvest_* (shared) ──→ sales_transform_<impl> ──→ sales_output_<impl>
                       └──→ artworks_*_<impl> ──→ artworks_output_<impl>
```

**Implementations:**
| Suffix | Description | Features |
|--------|-------------|----------|
| (none) | Original with processor classes | Processor pattern |
| `_polars` | Pure Polars with intermediate steps | Lazy evaluation, visualization |
| `_duckdb` | Pure DuckDB SQL queries | SQL-based transforms |
| `_polars_fs` | Polars variant (different group) | Same logic, separate group |
| `_polars_ops` | Graph-backed assets with ops | Detailed observability |
| `_polars_multi` | Multi-asset pattern | Tightly coupled steps |

**Jobs:**
- `processors_pipeline` - Original implementation with processor classes
- `polars_pipeline` - Pure Polars with intermediate step assets
- `duckdb_pipeline` - Pure DuckDB SQL
- `polars_fs_pipeline` - Polars variant
- `polars_ops_pipeline` - Graph-backed assets (ops)
- `polars_multi_pipeline` - Multi-asset pattern

## Project Structure

```
honey_duck/
  __init__.py           # Package metadata
  defs/
    definitions.py      # Combined Dagster Definitions
    dlt_sources.py      # dlt source configuration
    dlt_assets.py       # dagster-dlt asset wrapper
    assets.py           # Original transform/output (processor classes)
    assets_polars.py    # Pure Polars implementation
    assets_pandas.py    # Pure Pandas implementation
    assets_duckdb.py    # Pure DuckDB SQL implementation
    assets_polars_fs.py # Polars with FilesystemIOManager
    resources.py        # Path constants and configuration
    constants.py        # Business constants (thresholds, tiers)
    schemas.py          # Pandera validation schemas
    jobs.py             # Job definitions
    checks.py           # Asset checks

cogapp_deps/            # Utilities (simulates external package)
  README.md             # Complete API reference
  dagster/              # Dagster helpers
    helpers.py          # track_timing, altair_to_metadata, table_preview_to_metadata
    io.py               # DuckDBPandasPolarsIOManager
  processors/           # DataFrame processors
    pandas/             # PandasReplaceOnConditionProcessor
    polars/             # PolarsFilterProcessor, PolarsStringProcessor
    duckdb/             # DuckDBJoinProcessor, DuckDBWindowProcessor

data/
  input/                # Source CSV files
  output/               # Generated output (*.duckdb, *.json)

dagster_home/           # Dagster persistence (run history, schedules, etc.)
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DAGSTER_HOME` | Dagster persistence directory (absolute path). Without this, run history uses temp storage. | (optional) |
| `HONEY_DUCK_DB_PATH` | DuckDB database path | `data/output/dagster.duckdb` |
| `HONEY_DUCK_SALES_OUTPUT` | Sales JSON output path | `data/output/sales_output.json` |
| `HONEY_DUCK_ARTWORKS_OUTPUT` | Artworks JSON output path | `data/output/artworks_output.json` |

Copy `.env.example` to `.env` and set `DAGSTER_HOME` to persist run history between sessions.

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

- **DuckDBQueryProcessor** - Query tables from the configured database
- **DuckDBSQLProcessor** - Transform DataFrames with SQL (in-memory)
- **DuckDBWindowProcessor** - Add window function columns
- **DuckDBAggregateProcessor** - GROUP BY aggregations
- **PolarsFilterProcessor** - Filter rows by condition
- **PolarsStringProcessor** - String transformations (strip, upper, lower)
- **Chain** - Compose Polars processors with lazy optimization

## Adding a New Asset

1. **Define the asset** in `honey_duck/defs/assets.py`:

```python
@dg.asset(
    kinds={"duckdb"},           # Shows up in UI as DuckDB asset
    deps=HARVEST_DEPS,          # Depends on raw tables
    group_name="transform",     # Groups in UI
)
def my_new_transform(context: dg.AssetExecutionContext):
    """Docstring shown in Dagster UI."""
    # Query raw tables
    result = DuckDBQueryProcessor(sql="""
        SELECT * FROM raw.my_table
    """).process()

    # Transform with Polars
    result = PolarsStringProcessor("name", "upper").process(result)

    # Add metadata for UI
    context.add_output_metadata({
        "record_count": len(result),
        "preview": dg.MetadataValue.md(result.head(5).to_pandas().to_markdown()),
    })
    return result
```

2. **Register it** in `honey_duck/defs/definitions.py`:

```python
from .assets import my_new_transform

defs = dg.Definitions(
    assets=[
        # ... existing assets
        my_new_transform,
    ],
    # ...
)
```

3. **Add to job** (optional) in `honey_duck/defs/jobs.py` if you want a separate execution target.

**Tips:**
- Return `pl.DataFrame` for Polars output, `pd.DataFrame` for pandas - the IO manager handles both
- Use `pl.DataFrame` type hints on input parameters so the IO manager knows what to load
- Add business constants to `honey_duck/defs/constants.py`
