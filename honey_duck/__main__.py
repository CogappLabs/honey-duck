"""Run a pipeline definition."""

import argparse
import importlib.util
from pathlib import Path


def load_pipeline_module(path: Path):
    """Load a pipeline definition module from path."""
    spec = importlib.util.spec_from_file_location("pipeline_def", path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a honey-duck pipeline")
    parser.add_argument(
        "pipeline",
        nargs="?",
        default="pipelines/full.py",
        help="Path to pipeline definition (default: pipelines/full.py)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show plan without running")
    parser.add_argument(
        "--db",
        default=None,
        help="DuckDB file path (default: in-memory, or use DB_PATH from pipeline)",
    )
    args = parser.parse_args()

    # Find pipeline file
    pipeline_path = Path(args.pipeline)
    if not pipeline_path.is_absolute():
        # Try relative to cwd, then package root
        if not pipeline_path.exists():
            pipeline_path = Path(__file__).parent.parent / args.pipeline

    if not pipeline_path.exists():
        print(f"Pipeline not found: {args.pipeline}")
        return

    # Load pipeline definition
    pipeline_def = load_pipeline_module(pipeline_path)

    print(f"Pipeline: {pipeline_path.stem}")
    print(f"Sources: {list(pipeline_def.SOURCES.keys())}")
    print(f"Processors: {len(pipeline_def.PROCESSORS)}")
    for i, p in enumerate(pipeline_def.PROCESSORS, 1):
        print(f"  {i}. {p.__class__.__name__}")
    print(f"Output: {pipeline_def.OUTPUT}")
    print()

    if args.dry_run:
        print("Dry run - not executing")
        return

    # Run pipeline
    from honey_duck import DuckDBPipeline

    # Determine DB path: CLI arg > pipeline definition > in-memory
    db_path = args.db or getattr(pipeline_def, "DB_PATH", None) or ":memory:"
    pipeline = DuckDBPipeline(db_path)

    if db_path != ":memory:":
        print(f"Using database: {db_path}")

    # Harvest sources
    for table_name, path in pipeline_def.SOURCES.items():
        suffix = Path(path).suffix.lower()
        if suffix == ".csv":
            pipeline.harvest_csv(str(path), table_name=table_name if table_name != "pipeline_data" else None)
        elif suffix == ".json":
            pipeline.harvest_json(str(path), table_name=table_name if table_name != "pipeline_data" else None)
        elif suffix == ".parquet":
            pipeline.harvest_parquet(str(path), table_name=table_name if table_name != "pipeline_data" else None)

    # Add processors
    for processor in pipeline_def.PROCESSORS:
        pipeline.add_processor(processor)

    # Run
    result = pipeline.run()
    print("Result:")
    print(result)
    print()

    # Output
    output_path = Path(pipeline_def.OUTPUT)
    suffix = output_path.suffix.lower()
    if suffix == ".json":
        pipeline.output_json(str(output_path))
    elif suffix == ".csv":
        pipeline.output_csv(str(output_path))
    elif suffix == ".parquet":
        pipeline.output_parquet(str(output_path))

    print(f"Written to: {output_path}")


if __name__ == "__main__":
    main()
