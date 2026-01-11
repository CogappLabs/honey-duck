"""Configuration management for the honey-duck pipeline.

This module provides validated configuration with clear error messages.
All configuration is loaded from environment variables with sensible defaults.
"""

import os
from dataclasses import dataclass
from pathlib import Path

from cogapp_deps.dagster import ConfigurationError


@dataclass(frozen=True)
class HoneyDuckConfig:
    """Validated pipeline configuration.

    All paths and values are validated at creation time to fail fast
    if configuration is invalid.

    Attributes:
        duckdb_path: Path to DuckDB database file
        parquet_dir: Directory for Parquet IO manager storage
        json_output_dir: Directory for JSON outputs
        min_sale_value_usd: Minimum sale value for filtering (default: 30M)
        price_tier_budget_max_usd: Upper bound for budget tier (default: 500K)
        price_tier_mid_max_usd: Upper bound for mid tier (default: 3M)
        freshness_hours: Hours before output is considered stale (default: 24)
    """

    duckdb_path: Path
    parquet_dir: Path
    json_output_dir: Path
    storage_dir: Path
    dlt_dir: Path
    min_sale_value_usd: int
    price_tier_budget_max_usd: int
    price_tier_mid_max_usd: int
    freshness_hours: int

    def validate(self) -> None:
        """Validate configuration at startup.

        Creates required directories if they don't exist.

        Raises:
            ConfigurationError: If any configuration is invalid
        """
        # Create directories if they don't exist
        try:
            self.parquet_dir.mkdir(parents=True, exist_ok=True)
            self.json_output_dir.mkdir(parents=True, exist_ok=True)
            self.storage_dir.mkdir(parents=True, exist_ok=True)
            self.dlt_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise ConfigurationError(
                f"Failed to create output directories: {e}. "
                f"Check permissions for parent directories."
            ) from e

        # Check directories are writable
        for path_name, path in [
            ("parquet_dir", self.parquet_dir),
            ("json_output_dir", self.json_output_dir),
        ]:
            if not os.access(path, os.W_OK):
                raise ConfigurationError(
                    f"{path_name} at {path} is not writable. "
                    f"Check permissions with: ls -ld {path}"
                )

        # Check DuckDB path parent is writable (file may not exist yet)
        if self.duckdb_path.parent.exists() and not os.access(
            self.duckdb_path.parent, os.W_OK
        ):
            raise ConfigurationError(
                f"DuckDB parent directory {self.duckdb_path.parent} is not writable"
            )

        # Validate numeric values
        if self.min_sale_value_usd < 0:
            raise ConfigurationError(
                f"min_sale_value_usd must be positive, got: {self.min_sale_value_usd}"
            )

        if self.price_tier_budget_max_usd <= 0:
            raise ConfigurationError(
                f"price_tier_budget_max_usd must be positive, got: {self.price_tier_budget_max_usd}"
            )

        if self.price_tier_mid_max_usd <= self.price_tier_budget_max_usd:
            raise ConfigurationError(
                f"price_tier_mid_max_usd ({self.price_tier_mid_max_usd}) must be "
                f"greater than price_tier_budget_max_usd ({self.price_tier_budget_max_usd})"
            )

        if self.freshness_hours <= 0:
            raise ConfigurationError(
                f"freshness_hours must be positive, got: {self.freshness_hours}"
            )

    @classmethod
    def from_env(cls) -> "HoneyDuckConfig":
        """Load and validate configuration from environment variables.

        Environment Variables:
            HONEY_DUCK_DB_PATH: DuckDB database path (default: data/output/dlt/dagster.duckdb)
            HONEY_DUCK_PARQUET_DIR: Parquet storage directory (default: data/output/storage)
            HONEY_DUCK_JSON_DIR: JSON output directory (default: data/output/json)
            MIN_SALE_VALUE_USD: Minimum sale value filter (default: 30000000)
            PRICE_TIER_BUDGET_MAX_USD: Budget tier max (default: 500000)
            PRICE_TIER_MID_MAX_USD: Mid tier max (default: 3000000)
            FRESHNESS_HOURS: Freshness window in hours (default: 24)

        Returns:
            Validated configuration object

        Raises:
            ConfigurationError: If configuration is invalid
        """
        # Resolve paths relative to project root
        project_root = Path(__file__).parent.parent.parent
        data_dir = project_root / "data"
        output_dir = data_dir / "output"

        config = cls(
            duckdb_path=Path(
                os.environ.get(
                    "HONEY_DUCK_DB_PATH", str(output_dir / "dlt" / "dagster.duckdb")
                )
            ),
            parquet_dir=Path(
                os.environ.get("HONEY_DUCK_PARQUET_DIR", str(output_dir / "storage"))
            ),
            json_output_dir=Path(
                os.environ.get("HONEY_DUCK_JSON_DIR", str(output_dir / "json"))
            ),
            storage_dir=Path(
                os.environ.get("HONEY_DUCK_STORAGE_DIR", str(output_dir / "storage"))
            ),
            dlt_dir=Path(
                os.environ.get("HONEY_DUCK_DLT_DIR", str(output_dir / "dlt"))
            ),
            min_sale_value_usd=int(
                os.environ.get("MIN_SALE_VALUE_USD", "30000000")
            ),
            price_tier_budget_max_usd=int(
                os.environ.get("PRICE_TIER_BUDGET_MAX_USD", "500000")
            ),
            price_tier_mid_max_usd=int(
                os.environ.get("PRICE_TIER_MID_MAX_USD", "3000000")
            ),
            freshness_hours=int(os.environ.get("FRESHNESS_HOURS", "24")),
        )

        # Validate before returning
        config.validate()

        return config


# Global configuration instance - validated on import
try:
    CONFIG = HoneyDuckConfig.from_env()
except ConfigurationError as e:
    # Re-raise with helpful context
    raise ConfigurationError(
        f"Failed to load configuration: {e}\n\n"
        f"Check your environment variables and directory permissions."
    ) from e
