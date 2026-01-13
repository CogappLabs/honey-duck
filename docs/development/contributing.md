# Contributing

Guidelines for contributing to Honey Duck.

## Development Setup

```bash
# Clone and install
git clone https://github.com/CogappLabs/honey-duck.git
cd honey-duck
uv sync

# Run tests
uv run pytest

# Run linting
uv run ruff check .
uv run ruff format .

# Type checking
uv run mypy src/honey_duck cogapp_deps
```

## Pre-commit Hooks

The project uses [lefthook](https://github.com/evilmartians/lefthook) for pre-commit hooks:

- **mypy** - Type checking
- **pytest** - Run tests
- **ruff** - Linting and formatting

Hooks run automatically on commit.

## Documentation

Build and preview docs locally:

```bash
# Install docs dependencies
uv sync --group docs

# Serve locally
uv run mkdocs serve

# Build static site
uv run mkdocs build
```

## Adding New Assets

1. Add asset function in appropriate `src/honey_duck/defs/<technology>/assets.py` file
2. Import `STANDARD_HARVEST_DEPS` from `helpers.py` for dependencies
3. Decorate with `@dg.asset(kinds={"polars"}, group_name="...", deps=...)`
4. Inject resources: `paths: PathsResource`, `output_paths: OutputPathsResource`
5. Add rich metadata via `context.add_output_metadata()`
6. Add to `definitions.py` assets list

## Code Style

- Follow [Polars patterns](../user-guide/polars-patterns.md) for DataFrame code
- Use type hints everywhere
- Write docstrings in Google style
- Keep assets focused and single-purpose
