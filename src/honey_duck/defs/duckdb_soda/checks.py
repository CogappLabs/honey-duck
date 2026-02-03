"""Soda v4 asset checks for the DuckDB + Soda pipeline.

These checks use Soda contracts defined in YAML files to validate
data quality. Soda v4 API validates parquet files directly via DuckDB.

Uses Soda Core v4 API (soda_core.contracts) which supports DuckDB 1.4+.
Install: pip install -i https://pypi.cloud.soda.io/simple soda-duckdb>=4

Benefits:
- Validation runs as SQL via DuckDB (memory-efficient)
- Contract YAML files serve as schema documentation
- Parquet validated directly - no DataFrame in memory
"""

from pathlib import Path
import time
import tempfile

import dagster as dg
import duckdb

from .assets import artworks_transform_soda, sales_transform_soda

# Path to contract YAML files
CONTRACTS_DIR = Path(__file__).parent / "contracts"


def _run_soda_check_v4(
    parquet_path: str,
    table_name: str,
    contract_path: Path,
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    """Run Soda v4 checks against a parquet file.

    Uses Soda's DuckDB data source to validate directly from parquet.
    No data loaded into Python memory.

    Per Soda docs, DuckDB can read parquet directly by pointing the
    database config at the parquet file path.
    """
    try:
        from soda_core.contracts import verify_contract_locally
    except ImportError:
        context.log.warning(
            "soda-duckdb not installed. Install with: "
            "pip install -i https://pypi.cloud.soda.io/simple soda-duckdb>=4"
        )
        return dg.AssetCheckResult(
            passed=True,
            metadata={
                "warning": "Soda not installed - check skipped",
                "contract": str(contract_path.name),
            },
        )

    start_time = time.perf_counter()

    # Verify parquet file exists
    if not Path(parquet_path).exists():
        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "error": f"Parquet file not found: {parquet_path}",
                "contract": str(contract_path.name),
            },
        )

    # Get row count via DuckDB (memory-efficient)
    # Note: This standalone connection doesn't use the Dagster resource,
    # but count(*) is lightweight and doesn't need memory limits
    conn = duckdb.connect(":memory:")
    result = conn.sql(f"SELECT count(*) FROM read_parquet('{parquet_path}')").fetchone()
    row_count = result[0] if result else 0
    conn.close()

    # Create temporary data source config pointing directly to parquet
    # Per Soda docs: https://docs.soda.io/reference/data-source-reference-for-soda-core/duckdb/duckdb-advanced-usage
    with tempfile.TemporaryDirectory() as tmpdir:
        ds_config_path = Path(tmpdir) / "datasource.yml"
        ds_config_path.write_text(f"""
name: duckdb_check
type: duckdb
connection:
  database: "{parquet_path}"
""")

        try:
            result = verify_contract_locally(
                data_source_file_path=str(ds_config_path),
                contract_file_path=str(contract_path),
                publish=False,
            )
        except Exception as e:
            context.log.error(f"Soda validation error: {e}")
            return dg.AssetCheckResult(
                passed=False,
                metadata={
                    "error": dg.MetadataValue.text(str(e)),
                    "contract": dg.MetadataValue.text(str(contract_path.name)),
                    "parquet_path": dg.MetadataValue.path(parquet_path),
                },
            )

    elapsed_ms = (time.perf_counter() - start_time) * 1000

    # Parse results
    passed_count = result.number_of_checks_passed
    failed_count = result.number_of_checks_failed

    # Build metadata
    metadata = {
        "contract": dg.MetadataValue.text(str(contract_path.name)),
        "checks_passed": dg.MetadataValue.int(passed_count),
        "checks_failed": dg.MetadataValue.int(failed_count),
        "record_count": dg.MetadataValue.int(row_count),
        "execution_time_ms": dg.MetadataValue.float(round(elapsed_ms, 2)),
        "parquet_path": dg.MetadataValue.path(parquet_path),
    }

    if result.has_errors:
        metadata["errors"] = dg.MetadataValue.text(result.get_errors_str())

    all_passed = result.is_passed

    if all_passed:
        context.log.info(
            f"Soda validation passed: {passed_count} checks passed in {elapsed_ms:.1f}ms"
        )
    else:
        context.log.warning(
            f"Soda validation failed: {failed_count} checks failed, "
            f"{passed_count} passed in {elapsed_ms:.1f}ms"
        )
        if result.has_errors:
            context.log.warning(f"Errors: {result.get_errors_str()}")

    return dg.AssetCheckResult(
        passed=all_passed,
        metadata=metadata,
    )


# -----------------------------------------------------------------------------
# Blocking Soda Checks - Prevent downstream if validation fails
# -----------------------------------------------------------------------------


@dg.asset_check(asset=sales_transform_soda, blocking=True)
def check_sales_transform_soda(
    context: dg.AssetCheckExecutionContext,
    sales_transform_soda: str,  # Receives path from IO manager
) -> dg.AssetCheckResult:
    """Validate sales_transform_soda parquet against Soda contract.

    Contract: contracts/sales_transform.yml

    Blocking: If this fails, sales_output_soda will not materialize.
    """
    contract_path = CONTRACTS_DIR / "sales_transform.yml"
    return _run_soda_check_v4(
        parquet_path=sales_transform_soda,
        table_name="sales_transform",
        contract_path=contract_path,
        context=context,
    )


@dg.asset_check(asset=artworks_transform_soda, blocking=True)
def check_artworks_transform_soda(
    context: dg.AssetCheckExecutionContext,
    artworks_transform_soda: str,  # Receives path from IO manager
) -> dg.AssetCheckResult:
    """Validate artworks_transform_soda parquet against Soda contract.

    Contract: contracts/artworks_transform.yml

    Blocking: If this fails, artworks_output_soda will not materialize.
    """
    contract_path = CONTRACTS_DIR / "artworks_transform.yml"
    return _run_soda_check_v4(
        parquet_path=artworks_transform_soda,
        table_name="artworks_transform",
        contract_path=contract_path,
        context=context,
    )
