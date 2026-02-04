#!/usr/bin/env python3
"""Compare HS vs Polars vs DuckDB outputs for processor equivalents.

Normalizes NaN/None and list-like values, compares row multisets on shared columns.
"""

from __future__ import annotations

from collections import Counter

import numpy as np
import pandas as pd
import polars as pl

import scripts.test_processor_equivalents as tpe


def normalize_value(value):
    try:
        if isinstance(value, (float, np.floating)) and np.isnan(value):
            return None
    except Exception:
        pass
    try:
        if value != value:
            return None
    except Exception:
        pass
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, np.ndarray):
        return tuple(normalize_value(x) for x in value.tolist())
    if isinstance(value, list):
        return tuple(normalize_value(x) for x in value)
    if isinstance(value, tuple):
        return tuple(normalize_value(x) for x in value)
    if isinstance(value, dict):
        return tuple(sorted((k, normalize_value(v)) for k, v in value.items()))
    return value


def normalize_df(df):
    if df is None:
        return None
    if isinstance(df, pl.DataFrame):
        return df.to_pandas()
    return df


def rows_multiset(df):
    if df is None:
        return Counter()
    cols = list(df.columns)
    rows = [
        tuple(normalize_value(value) for value in row)
        for row in df[cols].itertuples(index=False, name=None)
    ]
    return Counter(rows)


def compare(hs, other, other_name):
    report = []
    if hs is None or other is None:
        return report
    hs_cols = list(hs.columns)
    other_cols = list(other.columns)
    missing = [c for c in hs_cols if c not in other_cols]
    extra = [c for c in other_cols if c not in hs_cols]
    if missing:
        report.append(f"{other_name} missing columns vs HS: {missing}")
    if extra:
        report.append(f"{other_name} extra columns vs HS: {extra}")
    common = [c for c in hs_cols if c in other_cols]
    if not common:
        report.append(f"{other_name} shares no columns with HS")
        return report
    hs_common = hs[common]
    other_common = other[common]
    hs_rows = rows_multiset(hs_common)
    other_rows = rows_multiset(other_common)
    if hs_rows != other_rows:
        diff = hs_rows - other_rows
        diff2 = other_rows - hs_rows
        sample = list(diff.elements())[:3]
        sample2 = list(diff2.elements())[:3]
        report.append(
            f"{other_name} row mismatch vs HS on columns {common}. "
            f"HS-only sample: {sample}; {other_name}-only sample: {sample2}"
        )
    return report


def main():
    results = []
    for name, data, polars_fn, duckdb_fn, honeysuckle_fn, extra_data in tpe.get_all_tests():
        res = tpe.run_test(
            name,
            data,
            polars_fn,
            duckdb_fn,
            honeysuckle_fn,
            extra_data,
            verbose=False,
        )
        hs = normalize_df(res.honeysuckle_output)
        polars = normalize_df(res.polars_output)
        duckdb = normalize_df(res.duckdb_output)
        reports = []
        if hs is None:
            reports.append("HS output unavailable; cannot compare")
        else:
            reports.extend(compare(hs, polars, "Polars"))
            reports.extend(compare(hs, duckdb, "DuckDB"))
        if reports:
            results.append((name, reports))

    if not results:
        print("All processors match HS outputs (normalized, order-insensitive).")
        return

    print("Mismatches vs HS outputs:")
    for name, reports in results:
        print(f"- {name}")
        for report in reports:
            print(f"  - {report}")


if __name__ == "__main__":
    main()
