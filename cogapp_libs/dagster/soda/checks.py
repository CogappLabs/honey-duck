"""Soda Core check execution for Dagster.

Provides utilities to run Soda data quality checks and convert results
to Dagster AssetCheckResults.

Note: Uses soda-core v3.x API (from pypi.org). The contract-based API
in soda v4 (from pypi.cloud.soda.io) has a different structure.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import dagster as dg

if TYPE_CHECKING:
    from dagster_duckdb import DuckDBResource


@dataclass
class SodaCheckResult:
    """Result of Soda validation checks.

    Attributes:
        passed: Whether all checks passed
        checks_passed: Number of checks that passed
        checks_failed: Number of checks that failed
        checks_warned: Number of checks with warnings
        failures: List of failure details
        execution_time_ms: Time taken to run checks
        metadata: Additional metadata for Dagster
    """

    passed: bool
    checks_passed: int = 0
    checks_failed: int = 0
    checks_warned: int = 0
    failures: list[dict] = field(default_factory=list)
    execution_time_ms: float = 0.0
    metadata: dict = field(default_factory=dict)

    def to_dagster_result(self) -> dg.AssetCheckResult:
        """Convert to Dagster AssetCheckResult with rich metadata."""
        metadata = {
            "checks_passed": dg.MetadataValue.int(self.checks_passed),
            "checks_failed": dg.MetadataValue.int(self.checks_failed),
            "checks_warned": dg.MetadataValue.int(self.checks_warned),
            "execution_time_ms": dg.MetadataValue.float(self.execution_time_ms),
        }

        if self.failures:
            # Format failures as markdown table
            failure_lines = ["| Check | Value |", "|-------|-------|"]
            for f in self.failures[:10]:  # Limit to 10 for readability
                failure_lines.append(f"| {f.get('name', 'unknown')} | {f.get('value', 'N/A')} |")
            if len(self.failures) > 10:
                failure_lines.append(f"| ... | ({len(self.failures) - 10} more) |")
            metadata["failures"] = dg.MetadataValue.md("\n".join(failure_lines))

        # Add any extra metadata
        for key, value in self.metadata.items():
            if isinstance(value, (int, float, str, bool)):
                metadata[key] = value

        return dg.AssetCheckResult(
            passed=self.passed,
            metadata=metadata,
        )


def run_soda_checks(
    duckdb: DuckDBResource,
    table_name: str,
    checks_yaml: str,
    context: dg.AssetExecutionContext | None = None,
) -> SodaCheckResult:
    """Run Soda checks against a DuckDB table.

    Args:
        duckdb: DuckDB resource from Dagster
        table_name: Name of the table/view to validate
        checks_yaml: SodaCL YAML string with check definitions
        context: Optional Dagster context for logging

    Returns:
        SodaCheckResult with validation details
    """
    try:
        from soda.scan import Scan
    except ImportError as e:
        raise ImportError(
            "soda-core-duckdb is required. Install with: pip install soda-core-duckdb"
        ) from e

    start_time = time.perf_counter()

    scan = Scan()
    scan.set_scan_definition_name(f"validate_{table_name}")
    scan.set_data_source_name("duckdb")

    # Configure DuckDB data source
    scan.add_configuration_yaml_str("""
data_source duckdb:
  type: duckdb
""")

    # Get connection and register it
    with duckdb.get_connection() as conn:
        scan.add_duckdb_connection(conn)
        scan.add_sodacl_yaml_str(checks_yaml)

        # Execute scan
        scan.execute()

    elapsed_ms = (time.perf_counter() - start_time) * 1000

    # Parse results
    passed = 0
    failed = 0
    warned = 0
    failures = []

    for check in scan._checks:
        outcome = str(check.outcome).upper()
        if "PASS" in outcome:
            passed += 1
        elif "WARN" in outcome:
            warned += 1
        else:
            failed += 1
            failures.append(
                {
                    "name": check.name,
                    "value": getattr(check, "check_value", None),
                }
            )

    if context:
        if failed > 0:
            context.log.warning(f"Soda validation failed: {failed} checks failed")
        else:
            context.log.info(f"Soda validation passed: {passed} checks passed")

    return SodaCheckResult(
        passed=(failed == 0),
        checks_passed=passed,
        checks_failed=failed,
        checks_warned=warned,
        failures=failures,
        execution_time_ms=elapsed_ms,
    )


def run_soda_checks_on_parquet(
    duckdb: DuckDBResource,
    parquet_path: str | Path,
    view_name: str,
    checks_yaml: str,
    context: dg.AssetExecutionContext | None = None,
) -> SodaCheckResult:
    """Run Soda checks against a Parquet file via DuckDB.

    Creates a temporary view over the Parquet file and validates it.
    The data is never fully loaded into Python memory.

    Args:
        duckdb: DuckDB resource from Dagster
        parquet_path: Path to Parquet file (supports globs)
        view_name: Name for the temporary view
        checks_yaml: SodaCL YAML string with check definitions
        context: Optional Dagster context for logging

    Returns:
        SodaCheckResult with validation details
    """
    try:
        from soda.scan import Scan
    except ImportError as e:
        raise ImportError(
            "soda-core-duckdb is required. Install with: pip install soda-core-duckdb"
        ) from e

    start_time = time.perf_counter()

    scan = Scan()
    scan.set_scan_definition_name(f"validate_{view_name}")
    scan.set_data_source_name("duckdb")

    scan.add_configuration_yaml_str("""
data_source duckdb:
  type: duckdb
""")

    with duckdb.get_connection() as conn:
        # Create view over parquet file
        conn.execute(f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM read_parquet('{parquet_path}')
        """)

        # Get row count for metadata
        row_count = conn.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]

        scan.add_duckdb_connection(conn)
        scan.add_sodacl_yaml_str(checks_yaml)

        scan.execute()

    elapsed_ms = (time.perf_counter() - start_time) * 1000

    # Parse results
    passed = 0
    failed = 0
    warned = 0
    failures = []

    for check in scan._checks:
        outcome = str(check.outcome).upper()
        if "PASS" in outcome:
            passed += 1
        elif "WARN" in outcome:
            warned += 1
        else:
            failed += 1
            failures.append(
                {
                    "name": check.name,
                    "value": getattr(check, "check_value", None),
                }
            )

    if context:
        if failed > 0:
            context.log.warning(f"Soda validation failed: {failed} checks failed")
        else:
            context.log.info(f"Soda validation passed: {passed} checks passed")

    return SodaCheckResult(
        passed=(failed == 0),
        checks_passed=passed,
        checks_failed=failed,
        checks_warned=warned,
        failures=failures,
        execution_time_ms=elapsed_ms,
        metadata={"row_count": row_count},
    )


def generate_checks_yaml(
    table_name: str,
    required_columns: list[str] | None = None,
    nullable_columns: list[str] | None = None,
    positive_columns: list[str] | None = None,
    enum_columns: dict[str, list] | None = None,
    min_row_count: int = 1,
) -> str:
    """Generate SodaCL YAML from common validation patterns.

    Utility function to programmatically build check definitions.

    Args:
        table_name: Name of the table to validate
        required_columns: Columns that must not be null
        nullable_columns: Columns that can be null (for documentation)
        positive_columns: Numeric columns that must be > 0
        enum_columns: Dict of column -> allowed values
        min_row_count: Minimum expected row count

    Returns:
        SodaCL YAML string
    """
    lines = [f"checks for {table_name}:"]
    lines.append(f"  - row_count >= {min_row_count}")

    if required_columns:
        for col in required_columns:
            lines.append(f"  - missing_count({col}) = 0")

    if positive_columns:
        for col in positive_columns:
            lines.append(f"  - min({col}) > 0")

    if enum_columns:
        for col, values in enum_columns.items():
            values_str = ", ".join(f"'{v}'" for v in values)
            lines.append(f"  - invalid_count({col}) = 0:")
            lines.append(f"      valid values: [{values_str}]")

    return "\n".join(lines)
