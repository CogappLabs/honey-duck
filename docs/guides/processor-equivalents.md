---
title: Processor Equivalents
description: Replace Honeysuckle processors with native Polars expressions and DuckDB SQL for faster, simpler data transformations.
---

# Processor Equivalents

Common Honeysuckle processors and their native Polars/DuckDB equivalents.

!!! info "Why Native Operations?"
    Native Polars/DuckDB operations are:

    - **Faster**: No Python loop overhead, vectorized execution
    - **Simpler**: Less boilerplate, more readable
    - **Type-safe**: Better IDE support and error messages

!!! tip "Test Script"
    All examples are tested in [`scripts/test_processor_equivalents.py`](https://github.com/CogappLabs/honey-duck/blob/main/scripts/test_processor_equivalents.py).
    Run with: `uv run scripts/test_processor_equivalents.py --verbose`

## Documentation Links

- **Polars**: [docs.pola.rs](https://docs.pola.rs/) | [Expressions Guide](https://docs.pola.rs/user-guide/expressions/) | [API Reference](https://docs.pola.rs/api/python/stable/reference/)
- **DuckDB**: [duckdb.org/docs](https://duckdb.org/docs/) | [Functions](https://duckdb.org/docs/sql/functions/overview) | [Friendly SQL](https://duckdb.org/docs/sql/dialect/friendly_sql)
- **Honeysuckle**: [GitHub](https://github.com/Cogapp/honeysuckle) (private repo)

## Top 31 Processors by Usage

Based on analysis of Huntington Extractor, Universal Yiddish Library, and Royal Society SITM codebases.

| Rank | Processor | Usage | Operation |
|------|-----------|-------|-----------|
| 1 | `FillEmptyProcessor` | 55 | Fill nulls with column/constant |
| 2 | `RemoveMultivalueNullsProcessor` | 47 | Remove nulls from lists |
| 3 | `StripStringProcessor` | 44 | Strip whitespace |
| 4 | `ExtractFirstProcessor` | 42 | Get first element from list |
| 5 | `ExtractOnConditionProcessor` | 29 | Extract value conditionally |
| 6 | `ColumnsToDictsProcessor` | 27 | Combine columns into nested dicts |
| 7 | `ContainsBoolProcessor` | 26 | Check if string contains pattern |
| 8 | `StringConstantDataframeProcessor` | 23 | Add constant column |
| 9 | `AppendOnConditionProcessor` | 18 | Append to list conditionally |
| 10 | `ImplodeProcessor` | 15 | Aggregate values into lists |
| 11 | `ConcatProcessor` | 15 | Concatenate columns |
| 12 | `RenameProcessor` | 13 | Rename columns |
| 13 | `IsEmptyProcessor` | 13 | Check if column is null |
| 14 | `ExplodeColumnsProcessor` | 13 | Unnest lists to rows |
| 15 | `ReplaceOnConditionProcessor` | 10 | Conditional value replacement |
| 16 | `LowerStringProcessor` | 9 | Lowercase strings |
| 17 | `KeepOnlyProcessor` | 9 | Keep only specified columns |
| 18 | `DropNullColumnsProcessor` | 9 | Drop all-null columns |
| 19 | `DropColumnsProcessor` | 9 | Drop specified columns |
| 20 | `CopyFieldDataframeProcessor` | 9 | Copy column values |
| 21 | `ReplaceProcessor` | 8 | String replacement |
| 22 | `MergeProcessor` | 8 | Join dataframes |
| 23 | `FillMissingTuplesProcessor` | 8 | Fill missing struct fields |
| 24 | `ConditionalBoolProcessor` | 8 | Set boolean from expression |
| 25 | `CapitalizeProcessor` | 4 | Capitalize first character |
| 26 | `AppendStringProcessor` | 4 | Append/prepend string |
| 27 | `AddNumberProcessor` | 4 | Add value to column |
| 28 | `SubtractNumberProcessor` | 4 | Subtract value from column |
| 29 | `SplitStringProcessor` | 3 | Split string to list |
| 30 | `AsTypeProcessor` | — | Cast column types |
| 31 | `DropRowsOnConditionProcessor` | — | Filter rows by condition |

---

## 1. FillEmptyProcessor

Fill empty values with values from a different column or a constant.

| | |
|---|---|
| **Honeysuckle** | [`fill_empty_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/FillEmptyProcessor.py) |
| **Polars** | [`fill_null()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.fill_null.html) |
| **DuckDB** | [`coalesce()`](https://duckdb.org/docs/sql/functions/utility#coalesce) / [`ifnull()`](https://duckdb.org/docs/sql/functions/utility#ifnullexpr-other) |

```python
# Honeysuckle
FillEmptyProcessor(column_name="title", column_fill="alt_title")
FillEmptyProcessor(column_name="price", constant_fill=0)
FillEmptyProcessor(column_name="name", column_fill="display_name", constant_fill="Unknown")
```

### Example

| title | alt_title | | | title |
|-------|-----------|---|---|-------|
| `None` | "Backup" | → | | "Backup" |
| "Main" | "Backup" | → | | "Main" |

### Polars

```python
df = df.with_columns(
    pl.col("title").fill_null(pl.col("alt_title")),
    pl.col("price").fill_null(0),
)
```

Chained fallback:
```python
df = df.with_columns(
    pl.col("name").fill_null(pl.col("display_name")).fill_null("Unknown")
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:fill_empty_sql"
```

Chained coalesce:
```sql
SELECT coalesce(name, display_name, 'Unknown') AS name FROM items
```

---

## 2. RemoveMultivalueNullsProcessor

Remove null values from list/array columns.

| | |
|---|---|
| **Honeysuckle** | [`remove_multivalue_nulls_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/RemoveMultivalueNullsProcessor.py) |
| **Polars** | [`list.eval()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.list.eval.html) with [`drop_nulls()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.drop_nulls.html), [`list.len()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.list.len.html) |
| **DuckDB** | [`list_filter()`](https://duckdb.org/docs/sql/functions/list#list_filterlist-lambda) with lambda, [`nullif()`](https://duckdb.org/docs/sql/functions/utility#nullifexpr-value-to-null) |

```python
# Honeysuckle
RemoveMultivalueNullsProcessor(column="tags")
```

### Example

| tags | | | tags |
|------|---|---|------|
| `["a", None, "b"]` | → | | `["a", "b"]` |
| `[None, None]` | → | | `null` |

### Polars

```python
--8<-- "scripts/test_processor_equivalents.py:remove_nulls_polars"
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:remove_nulls_sql"
```

How it works:

- [`list_filter(tags, x -> x IS NOT NULL)`](https://duckdb.org/docs/sql/functions/list#list_filterlist-lambda) — Lambda filters nulls from list
- [`nullif(..., [])`](https://duckdb.org/docs/sql/functions/utility#nullifexpr-value-to-null) — Converts empty list `[]` to `NULL`

Alternative list comprehension:
```sql
SELECT nullif([x FOR x IN tags IF x IS NOT NULL], []) AS tags
FROM items
```

---

## 3. StripStringProcessor

Strip whitespace (or specific characters) from strings.

| | |
|---|---|
| **Honeysuckle** | [`strip_string_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/StripStringProcessor.py) |
| **Polars** | [`str.strip_chars()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.strip_chars.html) |
| **DuckDB** | [`.trim()`](https://duckdb.org/docs/sql/functions/text#trimstring-characters) (dot notation) |

```python
# Honeysuckle
StripStringProcessor(column_name="name", new_column="name_clean")
StripStringProcessor(column_name="code", new_column="code", strip_string="-")
```

### Example

| name | | | name_clean |
|------|---|---|------------|
| `"  hello  "` | → | | `"hello"` |
| `"\tworld\n"` | → | | `"world"` |

### Polars

```python
df = df.with_columns(pl.col("name").str.strip_chars().alias("name_clean"))
```

Additional patterns:
```python
# Strip specific character
df = df.with_columns(pl.col("code").str.strip_chars("-"))

# Strip only leading (start) or trailing (end)
df = df.with_columns(
    pl.col("name").str.strip_chars_start(),  # Left only
    pl.col("name").str.strip_chars_end(),    # Right only
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:strip_string_sql"
```

Additional patterns:
```sql
-- Strip specific character
SELECT trim(code, '-') AS code FROM items

-- Strip only leading/trailing
SELECT name.ltrim() AS name FROM items  -- Left only
SELECT name.rtrim() AS name FROM items  -- Right only
```

---

## 4. ExtractFirstProcessor

Extract first value from multivalue column.

| | |
|---|---|
| **Honeysuckle** | [`extract_first_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/ExtractFirstProcessor.py) |
| **Polars** | [`list.first()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.list.first.html) |
| **DuckDB** | [`list[1]`](https://duckdb.org/docs/sql/functions/list#listindex) (1-indexed) or [`list_extract()`](https://duckdb.org/docs/sql/functions/list#list_extractlist-index) |

```python
# Honeysuckle
ExtractFirstProcessor(column_name="authors", result_column="primary_author")
ExtractFirstProcessor(column_name="tags")  # Overwrites same column
```

### Example

| authors | | | primary_author |
|---------|---|---|----------------|
| `["Alice", "Bob"]` | → | | `"Alice"` |
| `["Charlie"]` | → | | `"Charlie"` |

### Polars

```python
df = df.with_columns(pl.col("authors").list.first().alias("primary_author"))
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:extract_first_sql"
```

Replace in-place:
```sql
SELECT * REPLACE (tags[1] AS tags) FROM items
```

---

## 5. ExtractOnConditionProcessor

Extract value from one column to another based on a condition.

| | |
|---|---|
| **Honeysuckle** | [`extract_on_condition_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/ExtractOnConditionProcessor.py) |
| **Polars** | [`when().then().otherwise()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.when.html) |
| **DuckDB** | [`CASE WHEN`](https://duckdb.org/docs/sql/expressions/case) or [`if()`](https://duckdb.org/docs/sql/functions/utility#ifcond-a-b) |

```python
# Honeysuckle
ExtractOnConditionProcessor(
    expression="is_on_sale == True",
    extract_columns=["price"],
    result_columns=["sale_price"],
)
```

### Example

| price | is_on_sale | | | sale_price |
|-------|------------|---|---|------------|
| 100 | `true` | → | | 100 |
| 200 | `false` | → | | `null` |

### Polars

```python
df = df.with_columns(
    pl.when(pl.col("is_on_sale")).then(pl.col("price")).otherwise(None).alias("sale_price")
)
```

Multiple conditions:
```python
df = df.with_columns(
    pl.when(pl.col("status") == "active")
      .then(pl.col("current_value"))
      .when(pl.col("status") == "pending")
      .then(pl.col("estimated_value"))
      .otherwise(None)
      .alias("display_value")
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:extract_on_condition_sql"
```

Multiple conditions with CASE:
```sql
SELECT CASE
    WHEN status = 'active' THEN current_value
    WHEN status = 'pending' THEN estimated_value
END AS display_value
FROM items
```

---

## 6. ContainsBoolProcessor

Check if a string contains a pattern and set boolean result.

| | |
|---|---|
| **Honeysuckle** | [`contains_bool_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/ContainsBoolProcessor.py) |
| **Polars** | [`str.contains()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.contains.html) |
| **DuckDB** | [`.contains()`](https://duckdb.org/docs/sql/functions/text#containsstring-search_string) (dot notation) or [`LIKE`/`ILIKE`](https://duckdb.org/docs/sql/functions/pattern_matching) |

```python
# Honeysuckle
ContainsBoolProcessor(column_name="description", result_column="has_keyword", pattern="important")
```

### Example

| description | | | has_keyword |
|-------------|---|---|-------------|
| `"This is important"` | → | | `true` |
| `"Nothing here"` | → | | `false` |

### Polars

```python
df = df.with_columns(pl.col("description").str.contains("important").alias("has_keyword"))
```

Regex pattern:
```python
df = df.with_columns(
    pl.col("email").str.contains(r"@gmail\.com$").alias("is_gmail")
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:contains_bool_sql"
```

Additional patterns:
```sql
-- Case-insensitive with ILIKE
SELECT description ILIKE '%important%' AS has_keyword FROM items

-- Regex pattern with regexp_matches()
SELECT regexp_matches(email, '@gmail\.com$') AS is_gmail FROM items
```

See also: [`regexp_matches()`](https://duckdb.org/docs/sql/functions/regular_expressions#regexp_matchesstring-pattern)

---

## 7. StringConstantDataframeProcessor

Add a column with a constant string value.

| | |
|---|---|
| **Honeysuckle** | [`string_constant_dataframe_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/StringConstantDataframeProcessor.py) |
| **Polars** | [`pl.lit()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.lit.html) |
| **DuckDB** | Literal value in SELECT |

```python
# Honeysuckle
StringConstantDataframeProcessor(target_field="source", value="huntington")
```

### Example

| id | | | id | source |
|----|---|---|----|--------|
| 1 | → | | 1 | `"huntington"` |
| 2 | → | | 2 | `"huntington"` |

### Polars

```python
df = df.with_columns(pl.lit("huntington").alias("source"))
```

Add multiple constants:
```python
df = df.with_columns(
    pl.lit("huntington").alias("source"),
    pl.lit("2.0").alias("version"),
    pl.lit(True).alias("is_published"),
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:string_constant_sql"
```

Add multiple constants:
```sql
SELECT *,
    'huntington' AS source,
    '2.0' AS version,
    true AS is_published
FROM items
```

---

## 8. AppendOnConditionProcessor

Append columns to a list/tuple based on a condition (uses pandas query syntax).

!!! warning "Inefficient Implementation"
    Honeysuckle uses `for i in range(len(data.index))` row-by-row iteration. The Polars/DuckDB equivalents are vectorized and significantly faster.

| | |
|---|---|
| **Honeysuckle** | [`append_on_condition_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/AppendOnConditionProcessor.py) |
| **Polars** | [`when().then()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.when.html) with [`list.concat()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.list.concat.html), [`concat_list()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.concat_list.html) |
| **DuckDB** | [`list_concat()`](https://duckdb.org/docs/sql/functions/list#list_concatlist1-list2) with `CASE WHEN` |

```python
# Honeysuckle
AppendOnConditionProcessor(
    initial_column="tags",
    append_columns=["category"],
    condition="is_featured == True"
)
```

### Example

| tags | category | is_featured | | | tags |
|------|----------|-------------|---|---|------|
| `["art"]` | `"new"` | `true` | → | | `["art", "new"]` |
| `["photo"]` | `"old"` | `false` | → | | `["photo"]` |

### Polars

```python
--8<-- "scripts/test_processor_equivalents.py:append_on_condition_polars"
```

Why the intermediate step? The [`list.concat()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.list.concat.html) method requires both operands to be lists. Using [`concat_list()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.concat_list.html) wraps the scalar in a list cleanly.

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:append_on_condition_sql"
```

---

## 9. ImplodeProcessor

Aggregate values into a list during group by.

| | |
|---|---|
| **Honeysuckle** | [`implode_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/ImplodeProcessor.py) |
| **Polars** | Automatic in [`group_by().agg()`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.group_by.html) |
| **DuckDB** | [`list()`](https://duckdb.org/docs/sql/functions/aggregates#listarg) with `GROUP BY ALL` |

```python
# Honeysuckle
ImplodeProcessor(column_names=["tag"], index_column="artwork_id")
```

### Example

| artwork_id | tag | | | artwork_id | tag |
|------------|-----|---|---|------------|------|
| 1 | `"oil"` | → | | 1 | `["oil", "portrait"]` |
| 1 | `"portrait"` | | | 2 | `["landscape"]` |
| 2 | `"landscape"` | | | | |

### Polars

```python
df = df.group_by("artwork_id").agg(pl.col("tag").alias("tag")).sort("artwork_id")
```

With ordering inside the list:
```python
df = df.group_by("artwork_id").agg(
    pl.col("filename").sort_by("sort_order").alias("media_files")
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:implode_sql"
```

With ordering inside the list:
```sql
SELECT artwork_id, list(filename ORDER BY sort_order) AS media_files
FROM media
GROUP BY ALL
```

---

## 10. ConcatProcessor

Concatenate multiple columns into a single string.

!!! warning "Inefficient Implementation"
    Honeysuckle uses `itertuples()` row-by-row iteration. The Polars/DuckDB equivalents are vectorized and significantly faster.

| | |
|---|---|
| **Honeysuckle** | [`concat_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/ConcatProcessor.py) |
| **Polars** | [`pl.concat_str()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.concat_str.html) |
| **DuckDB** | [`concat()`](https://duckdb.org/docs/sql/functions/text#concatstring-) or `||` operator |

```python
# Honeysuckle
ConcatProcessor(new_field="full_name", join_fields=["first", "last"], join_on=" ")
ConcatProcessor(new_field="code", join_fields=["prefix", "id"], join_on="-")
```

### Example

| first | last | | | full_name |
|-------|------|---|---|-----------|
| `"John"` | `"Doe"` | → | | `"John Doe"` |
| `"Jane"` | `"Smith"` | → | | `"Jane Smith"` |
| `null` | `"Unknown"` | → | | `"None Unknown"` |

### Polars

```python
df = df.with_columns(
    pl.concat_str(
        [pl.col("first").fill_null("None"), pl.col("last").fill_null("None")],
        separator=" ",
        ignore_nulls=False,
    ).alias("full_name")
)
```

Best practice - skip nulls instead of stringifying them:
```python
df = df.with_columns(
    pl.concat_str(["prefix", "id"], separator=" ", ignore_nulls=True).alias("code")
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:concat_sql"
```

Best practice - skip nulls with [`concat_ws()`](https://duckdb.org/docs/sql/functions/text#concat_wsseparator-string-):
```sql
SELECT concat_ws(' ', first, last) AS full_name FROM items
```

---

## 11. RenameProcessor

Rename columns in a dataframe.

| | |
|---|---|
| **Honeysuckle** | [`rename_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/RenameProcessor.py) |
| **Polars** | [`rename()`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.rename.html) |
| **DuckDB** | `AS` in SELECT |

```python
# Honeysuckle
RenameProcessor(mapping={"old_name": "new_name", "title": "artwork_title"})
```

### Example

| old_name | title | | | new_name | artwork_title |
|----------|-------|---|---|----------|---------------|
| `"value"` | `"Art"` | → | | `"value"` | `"Art"` |

### Polars

```python
df = df.rename({"old_name": "new_name", "title": "artwork_title"})
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:rename_sql"
```

Keep all columns, rename specific ones:
```sql
SELECT * REPLACE (title AS artwork_title) FROM items
```

---

## 12. IsEmptyProcessor

Check if column values are null and set boolean result.

| | |
|---|---|
| **Honeysuckle** | [`is_empty_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/IsEmptyProcessor.py) |
| **Polars** | [`is_null()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.is_null.html) / [`is_not_null()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.is_not_null.html) |
| **DuckDB** | `IS NULL` / `IS NOT NULL` |

```python
# Honeysuckle
IsEmptyProcessor(column_name="email", result_column="missing_email")
IsEmptyProcessor(column_name="email", result_column="has_email", invert=True)
```

### Example

| email | | | missing_email | has_email |
|-------|---|---|---------------|-----------|
| `null` | → | | `true` | `false` |
| `"a@b.com"` | → | | `false` | `true` |

### Polars

```python
df = df.with_columns(
    pl.col("email").is_null().alias("missing_email"),
    pl.col("email").is_not_null().alias("has_email"),
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:is_empty_sql"
```

---

## 13. ExplodeColumnsProcessor

Unnest/explode list columns into separate rows.

| | |
|---|---|
| **Honeysuckle** | [`explode_columns_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/ExplodeColumnsProcessor.py) |
| **Polars** | [`explode()`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.explode.html) |
| **DuckDB** | [`unnest()`](https://duckdb.org/docs/sql/functions/list#unnestlist) |

```python
# Honeysuckle
ExplodeColumnsProcessor(column_names=["tags"])
```

### Example

| id | tags | | | id | tags |
|----|------|---|---|----|------|
| 1 | `["a", "b"]` | → | | 1 | `"a"` |
| | | | | 1 | `"b"` |

### Polars

```python
df = df.explode("tags")
```

Explode multiple columns (must have same length lists):
```python
df = df.explode("tags", "tag_ids")
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:explode_sql"
```

Keep all other columns:
```sql
SELECT *, unnest(tags) AS tag FROM items
```

---

## 14. ReplaceOnConditionProcessor

Replace values based on a condition (==, !=, <, >, <=, >=).

| | |
|---|---|
| **Honeysuckle** | [`replace_on_condition_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/ReplaceOnConditionProcessor.py) |
| **Polars** | [`when().then().otherwise()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.when.html) |
| **DuckDB** | [`CASE WHEN`](https://duckdb.org/docs/sql/expressions/case) / [`greatest()`](https://duckdb.org/docs/sql/functions/utility#greatestx1-x2-) / [`least()`](https://duckdb.org/docs/sql/functions/utility#leastx1-x2-) |

```python
# Honeysuckle
ReplaceOnConditionProcessor(
    target_field="status",
    conditional_operator="==",
    conditional_value="",
    replacement="unknown"
)
ReplaceOnConditionProcessor(
    target_field="price",
    conditional_operator="<",
    conditional_value=0,
    replacement=0
)
```

### Example

| status | | | status |
|--------|---|---|--------|
| `""` | → | | `"unknown"` |
| `"active"` | → | | `"active"` |

### Polars

```python
df = df.with_columns(
    pl.when(pl.col("status") == "")
    .then(pl.lit("unknown"))
    .otherwise(pl.col("status"))
    .alias("status"),
    pl.max_horizontal(pl.col("price"), pl.lit(0)).alias("price"),
)
```

Note: [`max_horizontal()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.max_horizontal.html) returns the maximum value across columns, useful for clamping values.

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:replace_on_condition_sql"
```

Complex condition with CASE:
```sql
SELECT CASE WHEN price < 0 THEN 0 ELSE price END AS price FROM items
```

---

## 15. LowerStringProcessor

Convert strings to lowercase.

| | |
|---|---|
| **Honeysuckle** | [`lower_string_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/LowerStringProcessor.py) |
| **Polars** | [`str.to_lowercase()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.to_lowercase.html) |
| **DuckDB** | [`.lower()`](https://duckdb.org/docs/sql/functions/text#lowerstring) (dot notation) |

```python
# Honeysuckle
LowerStringProcessor(column_name="email")
```

### Example

| email | | | email |
|-------|---|---|-------|
| `"John@Example.COM"` | → | | `"john@example.com"` |

### Polars

```python
df = df.with_columns(pl.col("email").str.to_lowercase())
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:lower_string_sql"
```

Traditional function syntax:
```sql
SELECT lower(email) AS email FROM items
```

---

## 16. KeepOnlyProcessor

Keep only specified columns, drop all others.

| | |
|---|---|
| **Honeysuckle** | [`keep_only_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/KeepOnlyProcessor.py) |
| **Polars** | [`select()`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.select.html) |
| **DuckDB** | Column list in SELECT |

```python
# Honeysuckle
KeepOnlyProcessor(column_names=["id", "title"])
```

### Example

| id | title | author | internal_code | | | id | title |
|----|-------|--------|---------------|---|---|----|-------|
| 1 | "Art" | "Jane" | "X1" | → | | 1 | "Art" |

### Polars

```python
df = df.select(["id", "title"])
```

Using pl.col:
```python
df = df.select(pl.col("id", "title"))
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:keep_only_sql"
```

---

## 17. DropNullColumnsProcessor

Drop columns where ALL values are null.

| | |
|---|---|
| **Honeysuckle** | [`drop_null_columns_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/DropNullColumnsProcessor.py) |
| **Polars** | Filter columns using [`all()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.all.html) |
| **DuckDB** | Dynamic SQL (columns with all nulls) |

```python
# Honeysuckle
DropNullColumnsProcessor()  # No arguments
```

### Example

| id | empty_col | title | | | id | title |
|----|-----------|-------|---|---|----|-------|
| 1 | `null` | "Art" | → | | 1 | "Art" |
| 2 | `null` | "Photo" | | | 2 | "Photo" |

### Polars

```python
null_cols = [col for col in df.columns if df[col].is_null().all()]
df = df.drop(null_cols)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:drop_null_columns_sql"
```

---

## 18. DropColumnsProcessor

Drop specified columns from the dataframe.

| | |
|---|---|
| **Honeysuckle** | [`drop_columns_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/DropColumnsProcessor.py) |
| **Polars** | [`drop()`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.drop.html) |
| **DuckDB** | [`EXCLUDE`](https://duckdb.org/docs/sql/expressions/star#exclude-clause) |

```python
# Honeysuckle
DropColumnsProcessor(column_names=["internal_id"])
```

### Example

| id | internal_id | title | | | id | title |
|----|-------------|-------|---|---|----|-------|
| 1 | "abc123" | "Art" | → | | 1 | "Art" |

### Polars

```python
df = df.drop(["internal_id"])
```

Drop single column:
```python
df = df.drop("internal_id")
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:drop_columns_sql"
```

Or explicitly list columns to keep:
```sql
SELECT id, title FROM items
```

---

## 19. CopyFieldDataframeProcessor

Copy values from one column to another (create new column).

| | |
|---|---|
| **Honeysuckle** | [`copy_field_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/CopyFieldProcessor.py) |
| **Polars** | [`alias()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.alias.html) |
| **DuckDB** | `SELECT col AS new_col` |

```python
# Honeysuckle
CopyFieldDataframeProcessor(target_field="original_title", new_field="display_title")
```

### Example

| original_title | | | original_title | display_title |
|----------------|---|---|----------------|---------------|
| `"Mona Lisa"` | → | | `"Mona Lisa"` | `"Mona Lisa"` |

### Polars

```python
df = df.with_columns(pl.col("original_title").alias("display_title"))
```

Copy multiple columns:
```python
df = df.with_columns(
    pl.col("title").alias("display_title"),
    pl.col("date").alias("publication_date"),
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:copy_field_sql"
```

Copy multiple:
```sql
SELECT *, title AS display_title, date AS publication_date FROM items
```

---

## 20. ReplaceProcessor

Replace exact values in a column.

| | |
|---|---|
| **Honeysuckle** | [`replace_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/ReplaceProcessor.py) |
| **Polars** | [`when().then().otherwise()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.when.html) for exact value replacement |
| **DuckDB** | `CASE WHEN` for exact value replacement |

```python
# Honeysuckle
ReplaceProcessor(target_field="status", to_replace="", replacement="unknown")
ReplaceProcessor(target_field="category", to_replace="N/A", replacement=None)
```

### Example

| status | | | status |
|--------|---|---|--------|
| `""` | → | | `"unknown"` |

### Polars

```python
df = df.with_columns(
    pl.when(pl.col("status") == "")
    .then(pl.lit("unknown"))
    .otherwise(pl.col("status"))
    .alias("status")
)
```

Best practice for substring/regex replacement with [`str.replace_all()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.replace_all.html):
```python
df = df.with_columns(
    pl.col("text").str.replace_all(r"\s+", " ")  # Collapse whitespace
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:replace_sql"
```

Best practice for substring/regex replacement with [`regexp_replace()`](https://duckdb.org/docs/sql/functions/regular_expressions#regexp_replacestring-pattern-replacement):
```sql
SELECT regexp_replace(text, '\s+', ' ', 'g') AS text FROM items
```

---

## 21. MergeProcessor

Join/merge two dataframes together.

| | |
|---|---|
| **Honeysuckle** | [`merge_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/MergeProcessor.py) |
| **Polars** | [`join()`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.join.html) |
| **DuckDB** | [`JOIN`](https://duckdb.org/docs/sql/query_syntax/from#joins) |

```python
# Honeysuckle
MergeProcessor(
    merge_dataframe=artists_df,
    merge_columns=["artist_name", "birth_year"],
    merge_dataframe_left="artist_id",
    merge_dataframe_right="id",
    how="left"
)
```

### Example

**sales:**

| id | artist_id |
|----|-----------|
| 1 | 100 |

**artists:**

| id | artist_name |
|----|-------------|
| 100 | "Monet" |

**Result:**

| id_x | artist_id | artist_name | id_y |
|------|-----------|-------------|------|
| 1 | 100 | "Monet" | 100 |

### Polars

```python
--8<-- "scripts/test_processor_equivalents.py:merge_polars"
```

The [`suffix`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.join.html) parameter automatically appends a suffix to duplicate column names from the right DataFrame.

Additional patterns:
```python
# Inner join
df = sales.join(artists, left_on="artist_id", right_on="id", how="inner")

# Join with custom suffix for duplicate column names
df = sales.join(artists, left_on="artist_id", right_on="id", suffix="_artist")
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:merge_sql"
```

Additional patterns:
```sql
-- Inner join
SELECT s.*, a.artist_name
FROM sales s
INNER JOIN artists a ON s.artist_id = a.id

-- Using USING for same column name
SELECT * FROM sales JOIN artists USING (artist_id)
```

---

## 22. FillMissingTuplesProcessor

Fill missing tuple/list values in multiple columns by inserting nulls to match the row's tuple length.

!!! warning "Inefficient Implementation"
    Honeysuckle uses `itertuples()` row-by-row iteration with nested loops. The Polars/DuckDB equivalents are vectorized and significantly faster.

| | |
|---|---|
| **Honeysuckle** | [`fill_missing_tuples_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/FillMissingTuplesProcessor.py) |
| **Polars** | [`struct.field()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.struct.field.html) with [`fill_null()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.fill_null.html) |
| **DuckDB** | [Struct access](https://duckdb.org/docs/sql/data_types/struct#retrieving-from-structs) with `coalesce()` |

```python
# Honeysuckle
FillMissingTuplesProcessor(columns=["names", "roles"])
```

### Example

| names | roles | | | names | roles |
|-------|-------|---|---|-------|-------|
| `("Alice", "Bob")` | `null` | → | | `("Alice", "Bob")` | `(null, null)` |
| `null` | `("editor", "viewer")` | → | | `(null, null)` | `("editor", "viewer")` |

### Polars

```python
df = df.with_columns(
    pl.when(pl.col("names").is_null())
    .then(pl.lit([None, None]))
    .otherwise(pl.col("names"))
    .alias("names"),
    pl.when(pl.col("roles").is_null())
    .then(pl.lit([None, None]))
    .otherwise(pl.col("roles"))
    .alias("roles"),
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:fill_missing_tuples_sql"
```

---

## 23. ConditionalBoolProcessor

Evaluate a pandas query expression and set boolean result.

| | |
|---|---|
| **Honeysuckle** | [`conditional_bool_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/ConditionalBoolProcessor.py) |
| **Polars** | Boolean expressions |
| **DuckDB** | Boolean expressions |

```python
# Honeysuckle
ConditionalBoolProcessor(expression="price > 1000 and status == 'active'", result_column="is_premium")
```

### Example

| price | status | | | is_premium |
|-------|--------|---|---|------------|
| 1500 | `"active"` | → | | `true` |
| 500 | `"active"` | → | | `false` |

### Polars

```python
df = df.with_columns(
    ((pl.col("price") > 1000) & (pl.col("status") == "active")).alias("is_premium")
)
```

Complex expressions:
```python
df = df.with_columns(
    (
        (pl.col("price") > 1000) |
        (pl.col("featured") == True)
    ).alias("is_highlight")
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:conditional_bool_sql"
```

Complex expressions:
```sql
SELECT *, (price > 1000 OR featured = true) AS is_highlight FROM items
```

---

## 24. CapitalizeProcessor

Capitalize the first character of strings.

| | |
|---|---|
| **Honeysuckle** | [`capitalize_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/CapitalizeProcessor.py) |
| **Polars** | [`str.head()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.head.html) + [`str.to_uppercase()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.to_uppercase.html), or [`str.to_titlecase()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.to_titlecase.html) |
| **DuckDB** | [`initcap()`](https://duckdb.org/docs/sql/functions/text#initcapstring) |

```python
# Honeysuckle
CapitalizeProcessor(column_name="title")
```

### Example

| title | | | title |
|-------|---|---|-------|
| `"hello world"` | → | | `"Hello world"` |

### Polars

```python
--8<-- "scripts/test_processor_equivalents.py:capitalize_polars"
```

Note: [`str.head(1)`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.head.html) is cleaner than `.str.slice(0, 1)` for extracting the first character.

Title case (capitalize each word):
```python
df = df.with_columns(
    pl.col("title").str.to_titlecase()
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:capitalize_sql"
```

How it works:

- `title[1]` — DuckDB uses 1-based indexing for strings
- `title[2:]` — Slice from second character to end

Title case (capitalize each word):
```sql
SELECT initcap(title) AS title FROM items
```

---

## 25. AppendStringProcessor

Append or prepend a string to column values.

| | |
|---|---|
| **Honeysuckle** | [`append_string_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/AppendStringProcessor.py) |
| **Polars** | [`+` operator](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.__add__.html) or [`pl.concat_str()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.concat_str.html) |
| **DuckDB** | `||` operator or [`concat()`](https://duckdb.org/docs/sql/functions/text#concatstring-) |

```python
# Honeysuckle
AppendStringProcessor(column="id", value="ID-", prefix=True)   # Prepend
AppendStringProcessor(column="filename", value=".jpg")          # Append
```

### Example

| id | | | id |
|----|---|---|----|
| `"123"` | → | | `"ID-123"` |

### Polars

```python
df = df.with_columns(
    (pl.lit("ID-") + pl.col("id")).alias("id"),
    (pl.col("filename") + pl.lit(".jpg")).alias("filename"),
)
```

Using concat_str:
```python
df = df.with_columns(
    pl.concat_str([pl.lit("ID-"), pl.col("id")]).alias("id")
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:append_string_sql"
```

Using concat:
```sql
SELECT concat('ID-', id) AS id FROM items
```

---

## 26. AddNumberProcessor

Add a numeric value to column values.

| | |
|---|---|
| **Honeysuckle** | [`add_number_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/AddNumberProcessor.py) |
| **Polars** | [`+` operator](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.__add__.html) |
| **DuckDB** | `+` operator |

```python
# Honeysuckle
AddNumberProcessor(column_name="year", new_column="next_year", value=1)
```

### Example

| year | | | next_year |
|------|---|---|-----------|
| 2024 | → | | 2025 |

### Polars

```python
df = df.with_columns((pl.col("year") + 1).alias("next_year"))
```

Add to same column:
```python
df = df.with_columns(
    pl.col("price") + 10
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:add_number_sql"
```

Add to computed expression:
```sql
SELECT *, price + tax AS total FROM items
```

---

## 27. SubtractNumberProcessor

Subtract a numeric value from column values.

| | |
|---|---|
| **Honeysuckle** | [`subtract_number_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/SubtractNumberProcessor.py) |
| **Polars** | [`-` operator](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.__sub__.html) |
| **DuckDB** | `-` operator |

```python
# Honeysuckle
SubtractNumberProcessor(column_name="year", new_column="prev_year", value=1)
```

### Example

| year | | | prev_year |
|------|---|---|-----------|
| 2024 | → | | 2023 |

### Polars

```python
df = df.with_columns((pl.col("year") - 1).alias("prev_year"))
```

Calculate difference:
```python
df = df.with_columns(
    (pl.col("sale_price") - pl.col("cost")).alias("profit")
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:subtract_number_sql"
```

Calculate difference:
```sql
SELECT *, sale_price - cost AS profit FROM items
```

---

## 28. SplitStringProcessor

Split string into a list based on delimiter.

| | |
|---|---|
| **Honeysuckle** | [`split_string_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/SplitStringProcessor.py) |
| **Polars** | [`str.split()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.split.html) |
| **DuckDB** | [`string_split()`](https://duckdb.org/docs/sql/functions/text#string_splitstring-separator) |

```python
# Honeysuckle
SplitStringProcessor(column_name="tags", delimiter=",")
```

### Example

| tags | | | tags |
|------|---|---|------|
| `"a,b,c"` | → | | `["a", "b", "c"]` |

### Polars

```python
df = df.with_columns(pl.col("tags").str.split(","))
```

Split with default space delimiter:
```python
df = df.with_columns(
    pl.col("full_name").str.split(" ")
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:split_string_sql"
```

Split with default space:
```sql
SELECT string_split(full_name, ' ') AS name_parts FROM items
```

---

## 29. AsTypeProcessor

Cast column to a different data type.

| | |
|---|---|
| **Honeysuckle** | [`as_type_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/AsTypeProcessor.py) |
| **Polars** | [`cast()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.cast.html) |
| **DuckDB** | [`::TYPE`](https://duckdb.org/docs/sql/expressions/cast) or [`CAST()`](https://duckdb.org/docs/sql/expressions/cast) |

```python
# Honeysuckle
AsTypeProcessor(column_name="year", new_type="int")
AsTypeProcessor(column_name="price", new_type="float")
```

### Example

| year | | | year |
|------|---|---|------|
| `"2024"` | → | | `2024` (int) |

### Polars

```python
df = df.with_columns(
    pl.col("year").cast(pl.Int64),
    pl.col("price").cast(pl.Float64),
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:as_type_sql"
```

Alternative CAST syntax:
```sql
SELECT CAST(year AS INTEGER) AS year FROM items
```

---

## 30. DropRowsOnConditionProcessor

Filter/drop rows based on a condition.

| | |
|---|---|
| **Honeysuckle** | [`drop_rows_on_condition_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/DropRowsOnConditionProcessor.py) |
| **Polars** | [`filter()`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.filter.html) |
| **DuckDB** | [`WHERE`](https://duckdb.org/docs/sql/query_syntax/where) clause |

```python
# Honeysuckle
DropRowsOnConditionProcessor(target_field="status", conditional_operator="==", conditional_value="deleted")
DropRowsOnConditionProcessor(target_field="price", conditional_operator="<", conditional_value=0)
```

### Example

| status | price | | | status | price |
|--------|-------|---|---|--------|-------|
| `"active"` | 100 | → | | `"active"` | 100 |
| `"deleted"` | 50 | → | | (dropped) | |

### Polars

```python
df = df.filter((pl.col("status") != "deleted") & (pl.col("price") >= 0))
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:drop_rows_sql"
```

---

## 31. ColumnsToDictsProcessor

Combine multiple columns into a single column containing a list of dictionaries (nested structs). Useful for creating nested JSON output. Handles both single-value and multivalue columns.

| | |
|---|---|
| **Honeysuckle** | [`columns_to_dicts_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/ColumnsToDictsProcessor.py) |
| **Polars** | [`pl.struct()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.struct.html) wrapped in [`pl.concat_list()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.concat_list.html) |
| **DuckDB** | [Struct literals](https://duckdb.org/docs/sql/data_types/struct#creating-structs) in list |

```python
# Honeysuckle
ColumnsToDictsProcessor(
    new_field="dimensions",
    column_names=["width", "height", "depth"],
    new_column_names=["w", "h", "d"]  # Optional rename
)
```

### Example (single values)

| width | height | depth | | | dimensions |
|-------|--------|-------|---|---|------------|
| 10 | 20 | 5 | → | | `[{"w": 10, "h": 20, "d": 5}]` |
| 15 | 30 | 8 | → | | `[{"w": 15, "h": 30, "d": 8}]` |

### Example (multivalue - matching length lists)

| name | role | | | people |
|------|------|---|---|--------|
| `["Alice", "Bob"]` | `["admin", "user"]` | → | | `[{"name": "Alice", "role": "admin"}, {"name": "Bob", "role": "user"}]` |

### Polars

```python
df = df.with_columns(
    pl.concat_list(
        pl.struct(
            pl.col("width").alias("w"),
            pl.col("height").alias("h"),
            pl.col("depth").alias("d"),
        )
    ).alias("dimensions")
).drop(["width", "height", "depth"])
```

For multivalue columns (lists of same length):
```python
df = pl.DataFrame({
    "name": [["Alice", "Bob"]],
    "role": [["admin", "user"]]
})

# Explode, create struct, then group back
df = (
    df.with_row_index("_idx")
    .explode(["name", "role"])
    .with_columns(
        pl.struct(["name", "role"]).alias("person")
    )
    .group_by("_idx")
    .agg(pl.col("person").alias("people"))
    .drop("_idx")
)
```

### DuckDB

```sql
--8<-- "scripts/test_processor_equivalents.py:columns_to_dicts_sql"
```

Multivalue: transform parallel lists into list of structs using [`list_zip()`](https://duckdb.org/docs/sql/functions/list#list_ziplist1-list2-):
```sql
SELECT [
    {'name': name, 'role': role}
    FOR (name, role) IN list_zip(names, roles)
] AS people
FROM items
```

---

## Quick Reference

| Processor | Polars | DuckDB |
|-----------|--------|--------|
| `FillEmptyProcessor` | `.fill_null()` | `coalesce()` / `ifnull()` |
| `RemoveMultivalueNullsProcessor` | `.list.eval()` + `.list.len()` | `list_filter()` + `nullif()` |
| `StripStringProcessor` | `.str.strip_chars()` | `.trim()` |
| `ExtractFirstProcessor` | `.list.first()` | `col[1]` |
| `ExtractOnConditionProcessor` | `pl.when().then().otherwise()` | `if()` / `CASE WHEN` |
| `ContainsBoolProcessor` | `.str.contains()` | `.contains()` |
| `StringConstantDataframeProcessor` | `pl.lit()` | `'value' AS col` |
| `AppendOnConditionProcessor` | `concat_list()` + `.list.concat()` | `list_concat()` + `CASE` |
| `ImplodeProcessor` | `.group_by().agg()` | `list()` + `GROUP BY ALL` |
| `ConcatProcessor` | `pl.concat_str()` | `concat_ws()` / `||` |
| `RenameProcessor` | `.rename()` | `AS new_name` |
| `IsEmptyProcessor` | `.is_null()` / `.is_not_null()` | `IS NULL` / `IS NOT NULL` |
| `ExplodeColumnsProcessor` | `.explode()` | `unnest()` |
| `ReplaceOnConditionProcessor` | `pl.when().then().otherwise()` | `if()` / `greatest()` |
| `LowerStringProcessor` | `.str.to_lowercase()` | `.lower()` |
| `KeepOnlyProcessor` | `.select()` | Column list |
| `DropNullColumnsProcessor` | Filter columns | Dynamic SQL |
| `DropColumnsProcessor` | `.drop()` | `EXCLUDE` |
| `CopyFieldDataframeProcessor` | `.alias()` | `AS new_col` |
| `ReplaceProcessor` | `when().then().otherwise()` | `CASE WHEN` |
| `MergeProcessor` | `.join()` | `JOIN` |
| `FillMissingTuplesProcessor` | `.struct.field().fill_null()` | `coalesce(struct.field)` |
| `ConditionalBoolProcessor` | Boolean expression | Boolean expression |
| `CapitalizeProcessor` | `.str.head()` + `.str.to_uppercase()` | `upper(title[1]) || lower(title[2:])` |
| `AppendStringProcessor` | `+` / `pl.concat_str()` | `||` / `concat()` |
| `AddNumberProcessor` | `+` | `+` |
| `SubtractNumberProcessor` | `-` | `-` |
| `SplitStringProcessor` | `.str.split()` | `string_split()` |
| `AsTypeProcessor` | `.cast()` | `::TYPE` |
| `DropRowsOnConditionProcessor` | `.filter()` | `WHERE` |
| `ColumnsToDictsProcessor` | `pl.concat_list(pl.struct())` | `[{...} FOR ... IN list_zip()]` |
