"""Pandas replace-on-condition processor.

Generic processor to replace values based on a condition.
Follows the honeysuckle pattern from ReplaceOnConditionProcessor.
"""

from typing import Any

import pandas as pd


class PandasReplaceOnConditionProcessor:
    """Replace values in a column based on a condition.

    Can be chained to build tier classifications or any conditional replacement.

    Attributes:
        target_field: The column to update with replacement value.
        condition_target: The column to evaluate condition on (defaults to target_field).
        conditional_operator: One of '<', '<=', '>', '>=', '==', '!='.
        conditional_value: Value to compare against.
        replacement: Value to set when condition is true.

    Example (building price tiers by chaining):
        >>> # Start with premium as default
        >>> df['price_tier'] = 'premium'
        >>> # Replace with 'mid' where price < 100M
        >>> processor1 = PandasReplaceOnConditionProcessor(
        ...     target_field='price_tier',
        ...     condition_target='price_usd',
        ...     conditional_operator='<',
        ...     conditional_value=100_000_000,
        ...     replacement='mid'
        ... )
        >>> df = processor1.process(df)
        >>> # Replace with 'budget' where price < 20M
        >>> processor2 = PandasReplaceOnConditionProcessor(
        ...     target_field='price_tier',
        ...     condition_target='price_usd',
        ...     conditional_operator='<',
        ...     conditional_value=20_000_000,
        ...     replacement='budget'
        ... )
        >>> df = processor2.process(df)
    """

    def __init__(
        self,
        target_field: str,
        conditional_operator: str,
        conditional_value: Any,
        replacement: Any,
        condition_target: str | None = None,
    ) -> None:
        self.target_field = target_field
        self.conditional_operator = conditional_operator
        self.conditional_value = conditional_value
        self.replacement = replacement
        self.condition_target = condition_target or target_field

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply the conditional replacement to a DataFrame."""
        df = df.copy()

        col = df[self.condition_target]
        op = self.conditional_operator
        val = self.conditional_value

        if op == "<":
            mask = col.astype("float64") < val
        elif op == "<=":
            mask = col.astype("float64") <= val
        elif op == ">":
            mask = col.astype("float64") > val
        elif op == ">=":
            mask = col.astype("float64") >= val
        elif op == "==":
            mask = col == val
        elif op == "!=":
            mask = col != val
        else:
            raise ValueError(f"Invalid operator: {op}. Use <, <=, >, >=, ==, or !=")

        df.loc[mask, self.target_field] = self.replacement
        return df

    def __repr__(self) -> str:
        return (
            f"PandasReplaceOnConditionProcessor("
            f"{self.condition_target} {self.conditional_operator} {self.conditional_value} "
            f"-> {self.target_field}={self.replacement})"
        )
