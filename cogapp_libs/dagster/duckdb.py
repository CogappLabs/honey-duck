"""DuckDB asset factories for declarative SQL pipelines.

Provides factory functions to create Dagster assets from SQL queries without
boilerplate. Data flows through DuckDB/Parquet without materializing into Python.

Memory Safety:
    These factories are designed to never cause OOM, regardless of data size:

    - Views are lazy: CREATE VIEW doesn't load data, just defines the query
    - Streaming writes: conn.sql().write_parquet() streams directly to disk
    - COPY is streamed: COPY ... TO never loads full result into memory
    - DuckDB spills: Large intermediates spill to disk automatically

    The only Python memory used is:
    - Stats queries (single row: count, sum, etc.)
    - Example row for lineage (one row)
    - SQL strings

Factories:
    - duckdb_transform_asset: SQL → Parquet transform
    - duckdb_output_asset: Parquet → JSON output with optional filtering

Escape hatch:
    - DuckDBExecutor: Resource for complex multi-step operations
"""

import time
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_duckdb import DuckDBResource

from cogapp_libs.dagster.lineage import (
    collect_json_output_metadata,
    collect_parquet_metadata,
    get_example_row,
    register_harvest_views,
)


def duckdb_transform_asset(
    name: str,
    sql: str,
    *,
    harvest_views: list[str] | None = None,
    upstream: list[str] | None = None,
    lineage: dg.TableColumnLineage | None = None,
    example_id: tuple[str, int | str] | None = None,
    stats_sql: str | None = None,
    group_name: str = "transform",
    kinds: set[str] | None = None,
    check_fn: Any | None = None,
) -> dg.AssetsDefinition:
    """Create a DuckDB SQL transform asset.

    The asset executes SQL and writes results to Parquet. Sources (harvest views
    and upstream transforms) are registered as DuckDB views, so SQL can reference
    them by name.

    Args:
        name: Asset name (also used for output parquet filename)
        sql: SQL query to execute (can reference views by name)
        harvest_views: Harvest tables to register as views (e.g., ["sales", "artworks"])
        upstream: Upstream transform assets to depend on (registered as views)
        lineage: Column lineage definition for Dagster catalog
        example_id: (id_field, id_value) for example row in metadata
        stats_sql: Custom stats SQL (default: count + numeric sums from output)
        group_name: Dagster asset group
        kinds: Asset kinds (default: {"duckdb", "sql"})
        check_fn: Optional asset check function

    Returns:
        AssetsDefinition configured with ParquetPathIOManager

    Example:
        # First transform: reads from harvest
        sales_enriched = duckdb_transform_asset(
            name="sales_enriched",
            sql="SELECT s.*, a.title FROM sales s JOIN artworks a USING (artwork_id)",
            harvest_views=["sales", "artworks"],
            lineage=SALES_ENRICHED_LINEAGE,
            example_id=("sale_id", 2),
        )

        # Chained transform: reads from upstream
        sales_filtered = duckdb_transform_asset(
            name="sales_filtered",
            sql="SELECT * FROM sales_enriched WHERE sale_price_usd > 1000",
            upstream=["sales_enriched"],
        )

        # Mixed sources: upstream + harvest
        sales_with_artists = duckdb_transform_asset(
            name="sales_with_artists",
            sql="SELECT e.*, ar.name FROM sales_enriched e JOIN artists ar USING (artist_id)",
            upstream=["sales_enriched"],
            harvest_views=["artists"],
        )
    """
    asset_kinds = kinds or {"duckdb", "sql"}
    harvest_views = harvest_views or []
    upstream = upstream or []

    # Build deps: harvest deps if using harvest_views, plus any upstream assets
    deps: list[dg.AssetDep | str] = []
    if harvest_views:
        deps.extend(
            [
                dg.AssetDep("dlt_harvest_sales_raw"),
                dg.AssetDep("dlt_harvest_artworks_raw"),
                dg.AssetDep("dlt_harvest_artists_raw"),
                dg.AssetDep("dlt_harvest_media"),
            ]
        )

    # Upstream assets are handled via function parameters, not deps
    # (they're passed through ParquetPathIOManager)

    # Import resource types for proper Dagster injection
    # These are imported here to avoid circular imports at module level
    from honey_duck.defs.shared.resources import OutputPathsResource, PathsResource

    # Build the asset function dynamically based on upstream dependencies
    if upstream:
        # Has upstream dependencies - they come as string paths
        def make_asset_fn(
            upstream_names: list[str],
            paths_cls: type,
            output_paths_cls: type,
        ):
            # Create function with explicit upstream parameters
            def _asset_fn(context, duckdb, paths, output_paths, **kwargs) -> str:
                return _execute_transform(
                    context=context,
                    duckdb=duckdb,
                    paths=paths,
                    output_paths=output_paths,
                    name=name,
                    sql=sql,
                    harvest_views=harvest_views,
                    upstream_assets=kwargs,
                    lineage=lineage,
                    example_id=example_id,
                    stats_sql=stats_sql,
                )

            # Set proper annotations for Dagster resource injection
            # NOTE: upstream assets must be POSITIONAL_OR_KEYWORD (not KEYWORD_ONLY)
            # for Dagster to auto-wire them via the IO manager
            import inspect

            params = [
                inspect.Parameter(
                    "context",
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=dg.AssetExecutionContext,
                ),
                inspect.Parameter(
                    "duckdb", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=DuckDBResource
                ),
                inspect.Parameter(
                    "paths", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=paths_cls
                ),
                inspect.Parameter(
                    "output_paths",
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=output_paths_cls,
                ),
            ]
            # Add upstream assets as positional parameters for auto-wiring
            for upstream_name in upstream_names:
                params.append(
                    inspect.Parameter(
                        upstream_name, inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str
                    )
                )
            _asset_fn.__signature__ = inspect.Signature(params, return_annotation=str)  # type: ignore
            _asset_fn.__annotations__ = {
                "context": dg.AssetExecutionContext,
                "duckdb": DuckDBResource,
                "paths": paths_cls,
                "output_paths": output_paths_cls,
                "return": str,
                **{name: str for name in upstream_names},
            }
            return _asset_fn

        asset_fn = make_asset_fn(upstream, PathsResource, OutputPathsResource)
    else:
        # No upstream - simpler signature with proper type annotations
        def asset_fn(
            context: dg.AssetExecutionContext,
            duckdb: DuckDBResource,
            paths: PathsResource,
            output_paths: OutputPathsResource,
        ) -> str:
            return _execute_transform(
                context=context,
                duckdb=duckdb,
                paths=paths,
                output_paths=output_paths,
                name=name,
                sql=sql,
                harvest_views=harvest_views,
                upstream_assets={},
                lineage=lineage,
                example_id=example_id,
                stats_sql=stats_sql,
            )

    # Create the asset
    asset_def = dg.asset(
        name=name,
        kinds=asset_kinds,
        deps=deps if deps else None,
        group_name=group_name,
        io_manager_key="parquet_path_io_manager",
    )(asset_fn)

    return asset_def


def _execute_transform(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    paths: Any,
    output_paths: Any,
    name: str,
    sql: str,
    harvest_views: list[str],
    upstream_assets: dict[str, str],
    lineage: dg.TableColumnLineage | None,
    example_id: tuple[str, int | str] | None,
    stats_sql: str | None,
) -> str:
    """Execute the transform SQL and write to parquet."""
    start = time.perf_counter()

    # Determine output path
    output_path = Path(output_paths.transforms_soda_dir) / f"{name}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.get_connection() as conn:
        # Register harvest views
        if harvest_views:
            register_harvest_views(conn, paths.harvest_dir, harvest_views)

        # Register upstream assets as views
        for upstream_name, upstream_path in upstream_assets.items():
            conn.execute(
                f"CREATE OR REPLACE VIEW {upstream_name} AS SELECT * FROM '{upstream_path}'"
            )

        # Execute transform
        conn.sql(sql).write_parquet(str(output_path), compression="zstd")

        # Collect metadata
        elapsed_ms = (time.perf_counter() - start) * 1000

        if lineage:
            # Use provided stats SQL or generate default
            actual_stats_sql = (
                stats_sql
                or f"""
                SELECT count(*) AS record_count
                FROM '{output_path}'
            """
            )
            collect_parquet_metadata(
                context,
                conn,
                str(output_path),
                lineage=lineage,
                stats_sql=actual_stats_sql,
                example_id=example_id,
                elapsed_ms=elapsed_ms,
            )
        else:
            # Minimal metadata without lineage
            row_count = conn.sql(f"SELECT count(*) FROM '{output_path}'").fetchone()[0]
            context.add_output_metadata(
                {
                    "record_count": row_count,
                    "processing_time_ms": round(elapsed_ms, 2),
                }
            )

    context.log.info(f"Transformed {name} in {elapsed_ms:.1f}ms")
    return str(output_path)


def duckdb_output_asset(
    name: str,
    source: str,
    *,
    output_path_attr: str | None = None,
    output_format: str = "json",
    where: str | None = None,
    select: str = "*",
    lineage: dg.TableColumnLineage | None = None,
    example_id: tuple[str, int | str] | None = None,
    example_renames: dict[str, str] | None = None,
    group_name: str = "output",
    kinds: set[str] | None = None,
    deps: list[str] | None = None,
) -> dg.AssetsDefinition:
    """Create a DuckDB output asset that writes to JSON.

    Reads from an upstream transform's parquet and writes filtered/transformed
    output to JSON format.

    Args:
        name: Asset name
        source: Upstream transform asset name (provides parquet path)
        output_path_attr: Attribute name on OutputPathsResource for output path
            (e.g., "sales_soda" for OutputPathsResource.sales_soda)
        output_format: Output format ("json" supported)
        where: Optional SQL WHERE clause for filtering
        select: SQL SELECT clause (default "*", use for renames/excludes)
        lineage: Column lineage definition
        example_id: (id_field, id_value) for example row
        example_renames: Column renames to apply to example data
        group_name: Dagster asset group
        kinds: Asset kinds (default: {"duckdb", "json"})
        deps: Additional asset dependencies (e.g., for ordering)

    Returns:
        AssetsDefinition configured with ParquetPathIOManager

    Example:
        sales_output = duckdb_output_asset(
            name="sales_output_soda",
            source="sales_transform_soda",
            output_path_attr="sales_soda",
            where="sale_price_usd >= 50000",
            select="* EXCLUDE (price_diff), price_diff AS price_difference",
            lineage=SALES_OUTPUT_LINEAGE,
            example_id=("sale_id", 2),
            example_renames={"price_difference": "price_diff"},
            deps=["artworks_output_soda"],  # Ordering dependency
        )
    """
    # Import resource types for proper Dagster injection
    from honey_duck.defs.shared.resources import OutputPathsResource

    asset_kinds = kinds or {"duckdb", output_format}
    extra_deps = deps or []

    def asset_fn(
        context: dg.AssetExecutionContext,
        duckdb: DuckDBResource,
        output_paths: OutputPathsResource,
        **upstream: str,
    ) -> str:
        start = time.perf_counter()

        # Get source path from upstream
        source_path = upstream[source]

        # Determine output path
        if output_path_attr:
            output_path = getattr(output_paths, output_path_attr)
        else:
            # Fallback: try to derive from name (e.g., sales_output_soda -> sales_soda)
            attr_name = name.replace("_output", "")
            output_path = getattr(output_paths, attr_name, None)
            if not output_path:
                raise ValueError(
                    f"Could not determine output path for {name}. "
                    f"Please specify output_path_attr parameter."
                )
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        with duckdb.get_connection() as conn:
            # Build query
            query = f"SELECT {select} FROM '{source_path}'"
            if where:
                query += f" WHERE {where}"

            # Get counts for metadata
            total = conn.sql(f"SELECT count(*) FROM '{source_path}'").fetchone()[0]
            if where:
                filtered_count = conn.sql(
                    f"SELECT count(*) FROM '{source_path}' WHERE {where}"
                ).fetchone()[0]
            else:
                filtered_count = total

            # Write output
            conn.execute(f"COPY ({query}) TO '{output_path}' (FORMAT JSON, ARRAY true)")

            elapsed_ms = (time.perf_counter() - start) * 1000

            # Collect metadata
            if lineage:
                collect_json_output_metadata(
                    context,
                    conn,
                    input_path=source_path,
                    output_path=output_path,
                    lineage=lineage,
                    example_id=example_id,
                    example_renames=example_renames,
                    extra_metadata={
                        "record_count": filtered_count,
                        "filtered_from": total,
                    },
                    elapsed_ms=elapsed_ms,
                )
            else:
                context.add_output_metadata(
                    {
                        "record_count": filtered_count,
                        "filtered_from": total,
                        "json_output": dg.MetadataValue.path(output_path),
                        "processing_time_ms": round(elapsed_ms, 2),
                    }
                )

        context.log.info(f"Output {filtered_count} records to {name} in {elapsed_ms:.1f}ms")
        return source_path  # Return source path for downstream

    # Set up function signature for Dagster with proper type annotations
    # NOTE: upstream asset must be POSITIONAL_OR_KEYWORD (not KEYWORD_ONLY)
    # for Dagster to auto-wire it via the IO manager
    import inspect

    params = [
        inspect.Parameter(
            "context", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=dg.AssetExecutionContext
        ),
        inspect.Parameter(
            "duckdb", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=DuckDBResource
        ),
        inspect.Parameter(source, inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
        inspect.Parameter(
            "output_paths", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=OutputPathsResource
        ),
    ]
    asset_fn.__signature__ = inspect.Signature(params, return_annotation=str)  # type: ignore
    asset_fn.__annotations__ = {
        "context": dg.AssetExecutionContext,
        "duckdb": DuckDBResource,
        source: str,
        "output_paths": OutputPathsResource,
        "return": str,
    }

    return dg.asset(
        name=name,
        kinds=asset_kinds,
        deps=extra_deps if extra_deps else None,
        group_name=group_name,
        io_manager_key="parquet_path_io_manager",
    )(asset_fn)


class DuckDBExecutor(dg.ConfigurableResource):
    """Resource for complex DuckDB operations that don't fit the factory pattern.

    Use this as an escape hatch when you need:
    - Multi-step SQL operations
    - Conditional logic
    - Multiple outputs
    - Custom metadata handling

    Example:
        @dg.asset
        def complex_asset(context, executor: DuckDBExecutor, paths, output_paths) -> str:
            with executor.connection(paths.harvest_dir, ["sales", "artworks"]) as ctx:
                # Multi-step operations
                ctx.execute("CREATE TEMP TABLE stage1 AS SELECT ...")
                ctx.execute("CREATE TEMP TABLE stage2 AS SELECT ...")

                # Write final output
                output_path = output_paths.transforms_dir / "complex.parquet"
                ctx.write_parquet("SELECT * FROM stage2", output_path)

                # Custom metadata
                count = ctx.fetchone("SELECT count(*) FROM stage2")[0]
                context.add_output_metadata({"record_count": count})

                return str(output_path)
    """

    duckdb: DuckDBResource

    def connection(
        self,
        harvest_dir: str | None = None,
        harvest_views: list[str] | None = None,
        upstream_assets: dict[str, str] | None = None,
    ) -> "DuckDBContext":
        """Get a context manager for DuckDB operations.

        Args:
            harvest_dir: Base harvest directory for view registration
            harvest_views: Harvest tables to register as views
            upstream_assets: Upstream asset paths to register as views

        Returns:
            Context manager providing DuckDB operations
        """
        return DuckDBContext(
            duckdb=self.duckdb,
            harvest_dir=harvest_dir,
            harvest_views=harvest_views or [],
            upstream_assets=upstream_assets or {},
        )


class DuckDBContext:
    """Context manager for DuckDB operations."""

    def __init__(
        self,
        duckdb: DuckDBResource,
        harvest_dir: str | None,
        harvest_views: list[str],
        upstream_assets: dict[str, str],
    ):
        import duckdb as duckdb_module

        self._duckdb = duckdb
        self._harvest_dir = harvest_dir
        self._harvest_views = harvest_views
        self._upstream_assets = upstream_assets
        self._conn: duckdb_module.DuckDBPyConnection | None = None
        self._start_time: float | None = None

    def __enter__(self) -> "DuckDBContext":
        self._start_time = time.perf_counter()
        self._conn = self._duckdb.get_connection().__enter__()

        # Register harvest views
        if self._harvest_dir and self._harvest_views:
            register_harvest_views(self._conn, self._harvest_dir, self._harvest_views)

        # Register upstream assets
        for name, path in self._upstream_assets.items():
            self._conn.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM '{path}'")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn:
            self._conn.__exit__(exc_type, exc_val, exc_tb)

    @property
    def elapsed_ms(self) -> float:
        """Elapsed time since context entry in milliseconds."""
        if self._start_time:
            return (time.perf_counter() - self._start_time) * 1000
        return 0.0

    def _get_conn(self):
        """Get connection, raising if not in context."""
        if self._conn is None:
            raise RuntimeError("DuckDBContext must be used as a context manager")
        return self._conn

    def execute(self, sql: str) -> None:
        """Execute SQL statement."""
        self._get_conn().execute(sql)

    def sql(self, query: str):
        """Execute SQL query and return result."""
        return self._get_conn().sql(query)

    def fetchone(self, query: str) -> tuple:
        """Execute query and fetch one row."""
        return self._get_conn().sql(query).fetchone()

    def fetchall(self, query: str) -> list[tuple]:
        """Execute query and fetch all rows."""
        return self._get_conn().sql(query).fetchall()

    def write_parquet(
        self,
        sql: str,
        output_path: str | Path,
        compression: str = "zstd",
    ) -> str:
        """Execute SQL and write results to parquet.

        Args:
            sql: SQL query to execute
            output_path: Path to write parquet file
            compression: Compression algorithm (default: zstd)

        Returns:
            String path to written parquet file
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._get_conn().sql(sql).write_parquet(str(output_path), compression=compression)
        return str(output_path)

    def write_json(
        self,
        sql: str,
        output_path: str | Path,
    ) -> str:
        """Execute SQL and write results to JSON array.

        Args:
            sql: SQL query to execute
            output_path: Path to write JSON file

        Returns:
            String path to written JSON file
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._get_conn().execute(f"COPY ({sql}) TO '{output_path}' (FORMAT JSON, ARRAY true)")
        return str(output_path)

    def get_example_row(
        self,
        path: str,
        id_field: str | None = None,
        id_value: int | str | None = None,
    ) -> dict[str, str | None]:
        """Get formatted example row for lineage display."""
        return get_example_row(self._get_conn(), path, id_field, id_value)

    def register_view(self, name: str, path: str) -> None:
        """Register a parquet file as a DuckDB view."""
        self._get_conn().execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM '{path}'")


__all__ = [
    "duckdb_transform_asset",
    "duckdb_output_asset",
    "DuckDBExecutor",
    "DuckDBContext",
]
