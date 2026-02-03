"""Soda Core integration for Dagster asset checks.

This module provides utilities for running Soda data quality checks
as Dagster asset checks, using DuckDB as the execution engine.

Key Features:
- SQL-based validation (no data loaded into Python memory)
- Contract YAML files as schema documentation
- DuckDB handles memory management (spills to disk)
- Integrates with Dagster's asset check system

Usage:
    from cogapp_libs.dagster.soda import run_soda_checks, SodaCheckResult

    @dg.asset_check(asset=my_asset, blocking=True)
    def check_my_asset(duckdb: DuckDBResource) -> dg.AssetCheckResult:
        result = run_soda_checks(
            duckdb=duckdb,
            table_name="my_table",
            contract_path="contracts/my_contract.yml",
        )
        return result.to_dagster_result()
"""

from .checks import SodaCheckResult, run_soda_checks, run_soda_checks_on_parquet

__all__ = ["SodaCheckResult", "run_soda_checks", "run_soda_checks_on_parquet"]
