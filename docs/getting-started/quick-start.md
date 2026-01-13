# Quick Start

Get up and running with Honey Duck in under 5 minutes.

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager

## Installation

```bash
# Clone the repository
git clone https://github.com/CogappLabs/honey-duck.git
cd honey-duck

# Install dependencies
uv sync

# Optional: Enable persistent run history
cp .env.example .env
```

## Run the Pipeline

### Option 1: Dagster UI (Recommended)

```bash
uv run dg dev
```

Open [http://localhost:3000](http://localhost:3000) and click **"Materialize all"**.

### Option 2: CLI

```bash
uv run dg launch --job polars_pipeline
```

## Verify Output

Check that the pipeline created output files:

```bash
ls data/output/
# artworks_output_polars.json
# sales_output_polars.json
```

## Next Steps

- [Tutorial](tutorial.md) - Build your first asset
- [Best Practices](../user-guide/best-practices.md) - Production guidelines
- [API Reference](../api/index.md) - Complete API documentation
