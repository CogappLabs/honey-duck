# Processor Equivalents

Common Honeysuckle processors and their native Polars/DuckDB equivalents.

!!! info "Why Native Operations?"
    Native Polars/DuckDB operations are:

    - **Faster**: No Python loop overhead, vectorized execution
    - **Simpler**: Less boilerplate, more readable
    - **Type-safe**: Better IDE support and error messages

## Documentation Links

- **Polars**: [docs.pola.rs](https://docs.pola.rs/) | [Expressions Guide](https://docs.pola.rs/user-guide/expressions/) | [API Reference](https://docs.pola.rs/api/python/stable/reference/)
- **DuckDB**: [duckdb.org/docs](https://duckdb.org/docs/) | [Functions](https://duckdb.org/docs/sql/functions/overview) | [Friendly SQL](https://duckdb.org/docs/sql/dialect/friendly_sql)
- **Honeysuckle**: [GitHub](https://github.com/Cogapp/honeysuckle) (private repo)

## Top 30 Processors by Usage

Based on analysis of Huntington Extractor, Universal Yiddish Library, and Royal Society SITM codebases.

| Rank | Processor | Usage | Operation |
|------|-----------|-------|-----------|
| 1 | `FillEmptyProcessor` | 55 | Fill nulls with column/constant |
| 2 | `RemoveMultivalueNullsProcessor` | 47 | Remove nulls from lists |
| 3 | `StripStringProcessor` | 44 | Strip whitespace |
| 4 | `ExtractFirstProcessor` | 42 | Get first element from list |
| 5 | `ExtractOnConditionProcessor` | 29 | Extract value conditionally |
| 6 | `ContainsBoolProcessor` | 26 | Check if string contains pattern |
| 7 | `StringConstantDataframeProcessor` | 23 | Add constant column |
| 8 | `AppendOnConditionProcessor` | 18 | Append to list conditionally |
| 9 | `ImplodeProcessor` | 15 | Aggregate values into lists |
| 10 | `ConcatProcessor` | 15 | Concatenate columns |
| 11 | `RenameProcessor` | 13 | Rename columns |
| 12 | `IsEmptyProcessor` | 13 | Check if column is null |
| 13 | `ExplodeColumnsProcessor` | 13 | Unnest lists to rows |
| 14 | `ReplaceOnConditionProcessor` | 10 | Conditional value replacement |
| 15 | `LowerStringProcessor` | 9 | Lowercase strings |
| 16 | `KeepOnlyProcessor` | 9 | Keep only specified columns |
| 17 | `DropNullColumnsProcessor` | 9 | Drop all-null columns |
| 18 | `DropColumnsProcessor` | 9 | Drop specified columns |
| 19 | `CopyFieldDataframeProcessor` | 9 | Copy column values |
| 20 | `ReplaceProcessor` | 8 | String replacement |
| 21 | `MergeProcessor` | 8 | Join dataframes |
| 22 | `FillMissingTuplesProcessor` | 8 | Fill missing struct fields |
| 23 | `ConditionalBoolProcessor` | 8 | Set boolean from expression |
| 24 | `CapitalizeProcessor` | 4 | Capitalize first character |
| 25 | `AppendStringProcessor` | 4 | Append/prepend string |
| 26 | `AddNumberProcessor` | 4 | Add value to column |
| 27 | `SubtractNumberProcessor` | 4 | Subtract value from column |
| 28 | `SplitStringProcessor` | 3 | Split string to list |
| 29 | `AsTypeProcessor` | — | Cast column types |
| 30 | `DropRowsOnConditionProcessor` | — | Filter rows by condition |

---

## 1. FillEmptyProcessor

Fill empty values with values from a different column or a constant.

| | |
|---|---|
| **Honeysuckle** | [`fill_empty_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/fill_empty_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"title": [None, "Main"], "alt_title": ["Backup", "Backup"]})

# Fill from another column
df = df.with_columns(
    pl.col("title").fill_null(pl.col("alt_title"))
)

# Fill with constant
df = df.with_columns(
    pl.col("price").fill_null(0)
)

# Fill from column first, then constant for remaining nulls (chained)
df = df.with_columns(
    pl.col("name").fill_null(pl.col("display_name")).fill_null("Unknown")
)
```

### DuckDB

```sql
-- Fill from another column
SELECT coalesce(title, alt_title) AS title FROM items

-- Fill with constant
SELECT ifnull(price, 0) AS price FROM items

-- Fill from column first, then constant (chained coalesce)
SELECT coalesce(name, display_name, 'Unknown') AS name FROM items
```

---

## 2. RemoveMultivalueNullsProcessor

Remove null values from list/array columns.

| | |
|---|---|
| **Honeysuckle** | [`remove_multivalue_nulls_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/remove_multivalue_nulls_processor.py) |
| **Polars** | [`list.eval()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.list.eval.html) with [`drop_nulls()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.drop_nulls.html) |
| **DuckDB** | [`list_filter()`](https://duckdb.org/docs/sql/functions/list#list_filterlist-lambda) with lambda |

```python
# Honeysuckle
RemoveMultivalueNullsProcessor(column_name="tags")
```

### Example

| tags | | | tags |
|------|---|---|------|
| `["a", None, "b"]` | → | | `["a", "b"]` |
| `[None, None]` | → | | `[]` |

### Polars

```python
import polars as pl

# Input
df = pl.DataFrame({"tags": [["a", None, "b"], [None, None]]})

# Remove nulls from list column using list.eval()
df = df.with_columns(
    pl.col("tags").list.eval(pl.element().drop_nulls())
)
```

### DuckDB

```sql
-- Remove nulls using list_filter with lambda
SELECT list_filter(tags, x -> x IS NOT NULL) AS tags FROM items

-- Alternative: list comprehension syntax
SELECT [x FOR x IN tags IF x IS NOT NULL] AS tags FROM items
```

---

## 3. StripStringProcessor

Strip whitespace (or specific characters) from strings.

| | |
|---|---|
| **Honeysuckle** | [`strip_string_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/strip_string_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"name": ["  hello  ", "\tworld\n"]})

# Strip whitespace to new column
df = df.with_columns(
    pl.col("name").str.strip_chars().alias("name_clean")
)

# Strip specific character
df = df.with_columns(
    pl.col("code").str.strip_chars("-")
)

# Strip only leading (start) or trailing (end)
df = df.with_columns(
    pl.col("name").str.strip_chars_start(),  # Left only
    pl.col("name").str.strip_chars_end(),    # Right only
)
```

### DuckDB

```sql
-- Strip whitespace (dot notation - preferred)
SELECT name.trim() AS name_clean FROM items

-- Strip specific character
SELECT trim(code, '-') AS code FROM items

-- Chain with other string operations (dot notation)
SELECT name.trim().upper() AS name_clean FROM items

-- Strip only leading/trailing
SELECT name.ltrim() AS name FROM items  -- Left only
SELECT name.rtrim() AS name FROM items  -- Right only
```

---

## 4. ExtractFirstProcessor

Extract first value from multivalue column.

| | |
|---|---|
| **Honeysuckle** | [`extract_first_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/extract_first_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"authors": [["Alice", "Bob"], ["Charlie"]]})

# Extract first to new column
df = df.with_columns(
    pl.col("authors").list.first().alias("primary_author")
)

# Extract first, overwrite same column
df = df.with_columns(
    pl.col("tags").list.first()
)
```

### DuckDB

```sql
-- Extract first element (1-indexed in DuckDB)
SELECT authors[1] AS primary_author FROM items

-- Keep all columns, replace tags with first element
SELECT * REPLACE (tags[1] AS tags) FROM items
```

---

## 5. ExtractOnConditionProcessor

Extract value from one column to another based on a condition.

| | |
|---|---|
| **Honeysuckle** | [`extract_on_condition_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/extract_on_condition_processor.py) |
| **Polars** | [`when().then().otherwise()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.when.html) |
| **DuckDB** | [`CASE WHEN`](https://duckdb.org/docs/sql/expressions/case) or [`if()`](https://duckdb.org/docs/sql/functions/utility#ifcond-a-b) |

```python
# Honeysuckle
ExtractOnConditionProcessor(
    source_column="price",
    target_column="sale_price",
    condition_column="is_on_sale",
    condition_value=True
)
```

### Example

| price | is_on_sale | | | sale_price |
|-------|------------|---|---|------------|
| 100 | `true` | → | | 100 |
| 200 | `false` | → | | `null` |

### Polars

```python
import polars as pl

# Input
df = pl.DataFrame({"price": [100, 200], "is_on_sale": [True, False]})

# Extract value when condition is met
df = df.with_columns(
    pl.when(pl.col("is_on_sale"))
      .then(pl.col("price"))
      .otherwise(None)
      .alias("sale_price")
)

# Multiple conditions
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
-- Simple condition with if()
SELECT if(is_on_sale, price, NULL) AS sale_price FROM items

-- Multiple conditions with CASE
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
| **Honeysuckle** | [`contains_bool_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/contains_bool_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"description": ["This is important", "Nothing here"]})

# Simple contains check
df = df.with_columns(
    pl.col("description").str.contains("important").alias("has_keyword")
)

# Regex pattern
df = df.with_columns(
    pl.col("email").str.contains(r"@gmail\.com$").alias("is_gmail")
)
```

### DuckDB

```sql
-- Simple contains (dot notation - preferred)
SELECT description.contains('important') AS has_keyword FROM items

-- Case-insensitive with ILIKE
SELECT description ILIKE '%important%' AS has_keyword FROM items

-- Regex pattern
SELECT regexp_matches(email, '@gmail\.com$') AS is_gmail FROM items
```

---

## 7. StringConstantDataframeProcessor

Add a column with a constant string value.

| | |
|---|---|
| **Honeysuckle** | [`string_constant_dataframe_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/string_constant_dataframe_processor.py) |
| **Polars** | [`pl.lit()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.lit.html) |
| **DuckDB** | Literal value in SELECT |

```python
# Honeysuckle
StringConstantDataframeProcessor(column_name="source", value="huntington")
```

### Example

| id | | | id | source |
|----|---|---|----|--------|
| 1 | → | | 1 | `"huntington"` |
| 2 | → | | 2 | `"huntington"` |

### Polars

```python
import polars as pl

# Input
df = pl.DataFrame({"id": [1, 2]})

# Add constant column
df = df.with_columns(
    pl.lit("huntington").alias("source")
)

# Add multiple constants
df = df.with_columns(
    pl.lit("huntington").alias("source"),
    pl.lit("2.0").alias("version"),
    pl.lit(True).alias("is_published"),
)
```

### DuckDB

```sql
-- Add constant column
SELECT *, 'huntington' AS source FROM items

-- Add multiple constants
SELECT *,
    'huntington' AS source,
    '2.0' AS version,
    true AS is_published
FROM items
```

---

## 8. AppendOnConditionProcessor

Append columns to a list/tuple based on a condition (uses pandas query syntax).

| | |
|---|---|
| **Honeysuckle** | [`append_on_condition_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/append_on_condition_processor.py) |
| **Polars** | [`when().then()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.when.html) with [`list.concat()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.list.concat.html) |
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
import polars as pl

# Input
df = pl.DataFrame({
    "tags": [["art"], ["photo"]],
    "category": ["new", "old"],
    "is_featured": [True, False]
})

# Append category to tags when featured
df = df.with_columns(
    pl.when(pl.col("is_featured"))
      .then(pl.col("tags").list.concat(pl.col("category").cast(pl.List(pl.String))))
      .otherwise(pl.col("tags"))
      .alias("tags")
)
```

### DuckDB

```sql
-- Append category to tags list when featured
SELECT
    CASE WHEN is_featured
        THEN list_concat(tags, [category])
        ELSE tags
    END AS tags
FROM items
```

---

## 9. ImplodeProcessor

Aggregate values into a list during group by.

| | |
|---|---|
| **Honeysuckle** | [`implode_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/implode_processor.py) |
| **Polars** | Automatic in [`group_by().agg()`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.group_by.html) |
| **DuckDB** | [`list()`](https://duckdb.org/docs/sql/functions/aggregates#listarg) with `GROUP BY ALL` |

```python
# Honeysuckle (used in group_by context)
ImplodeProcessor(column_name="tag", result_column="tags")
```

### Example

| artwork_id | tag | | | artwork_id | tags |
|------------|-----|---|---|------------|------|
| 1 | `"oil"` | → | | 1 | `["oil", "portrait"]` |
| 1 | `"portrait"` | | | 2 | `["landscape"]` |
| 2 | `"landscape"` | | | | |

### Polars

```python
import polars as pl

# Input
df = pl.DataFrame({
    "artwork_id": [1, 1, 2],
    "tag": ["oil", "portrait", "landscape"]
})

# Aggregate to list during group_by (automatic)
df = df.group_by("artwork_id").agg(
    pl.col("tag").alias("tags")  # Creates list automatically
)

# With ordering inside the list
df = df.group_by("artwork_id").agg(
    pl.col("filename").sort_by("sort_order").alias("media_files")
)
```

### DuckDB

```sql
-- Aggregate to list with GROUP BY ALL
SELECT artwork_id, list(tag) AS tags
FROM item_tags
GROUP BY ALL

-- With ordering inside the list
SELECT artwork_id, list(filename ORDER BY sort_order) AS media_files
FROM media
GROUP BY ALL
```

---

## 10. ConcatProcessor

Concatenate multiple columns into a single string.

| | |
|---|---|
| **Honeysuckle** | [`concat_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/concat_processor.py) |
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

### Polars

```python
import polars as pl

# Input
df = pl.DataFrame({"first": ["John", "Jane"], "last": ["Doe", "Smith"]})

# Concatenate with separator
df = df.with_columns(
    pl.concat_str(["first", "last"], separator=" ").alias("full_name")
)

# Concatenate without separator
df = df.with_columns(
    pl.concat_str(["prefix", "id"]).alias("code")
)

# Handle nulls (skip_null_columns equivalent)
df = df.with_columns(
    pl.concat_str(["first", "last"], separator=" ", ignore_nulls=True).alias("full_name")
)
```

### DuckDB

```sql
-- Concatenate with separator (using concat_ws)
SELECT concat_ws(' ', first, last) AS full_name FROM items

-- Concatenate without separator
SELECT concat(prefix, id) AS code FROM items

-- Using || operator
SELECT first || ' ' || last AS full_name FROM items

-- With null handling (concat_ws ignores nulls)
SELECT concat_ws(' ', first, middle, last) AS full_name FROM items
```

---

## 11. RenameProcessor

Rename columns in a dataframe.

| | |
|---|---|
| **Honeysuckle** | [`rename_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/rename_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"old_name": ["value"], "title": ["Art"]})

# Rename columns with dict
df = df.rename({"old_name": "new_name", "title": "artwork_title"})

# Rename single column
df = df.rename({"old_name": "new_name"})
```

### DuckDB

```sql
-- Rename columns
SELECT old_name AS new_name, title AS artwork_title FROM items

-- Keep all columns, rename specific ones
SELECT * REPLACE (title AS artwork_title) FROM items
```

---

## 12. IsEmptyProcessor

Check if column values are null and set boolean result.

| | |
|---|---|
| **Honeysuckle** | [`is_empty_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/is_empty_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"email": [None, "a@b.com"]})

# Check if null
df = df.with_columns(
    pl.col("email").is_null().alias("missing_email")
)

# Check if not null (invert=True)
df = df.with_columns(
    pl.col("email").is_not_null().alias("has_email")
)
```

### DuckDB

```sql
-- Check if null
SELECT email IS NULL AS missing_email FROM items

-- Check if not null (invert=True)
SELECT email IS NOT NULL AS has_email FROM items
```

---

## 13. ExplodeColumnsProcessor

Unnest/explode list columns into separate rows.

| | |
|---|---|
| **Honeysuckle** | [`explode_columns_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/explode_columns_processor.py) |
| **Polars** | [`explode()`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.explode.html) |
| **DuckDB** | [`unnest()`](https://duckdb.org/docs/sql/functions/list#unnestlist) |

```python
# Honeysuckle
ExplodeColumnsProcessor(column_name="tags")
```

### Example

| id | tags | | | id | tags |
|----|------|---|---|----|------|
| 1 | `["a", "b"]` | → | | 1 | `"a"` |
| | | | | 1 | `"b"` |

### Polars

```python
import polars as pl

# Input
df = pl.DataFrame({"id": [1], "tags": [["a", "b"]]})

# Explode single column
df = df.explode("tags")

# Explode multiple columns (must have same length lists)
df = df.explode("tags", "tag_ids")
```

### DuckDB

```sql
-- Explode/unnest column
SELECT id, unnest(tags) AS tag FROM items

-- Keep all other columns
SELECT *, unnest(tags) AS tag FROM items
```

---

## 14. ReplaceOnConditionProcessor

Replace values based on a condition (==, !=, <, >, <=, >=).

| | |
|---|---|
| **Honeysuckle** | [`replace_on_condition_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/replace_on_condition_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"status": ["", "active"], "price": [-10, 50]})

# Replace empty strings
df = df.with_columns(
    pl.when(pl.col("status") == "")
      .then(pl.lit("unknown"))
      .otherwise(pl.col("status"))
      .alias("status")
)

# Replace negative values (floor at 0)
df = df.with_columns(
    pl.max_horizontal(pl.col("price"), pl.lit(0)).alias("price")
)
```

### DuckDB

```sql
-- Replace empty strings
SELECT if(status = '', 'unknown', status) AS status FROM items

-- Replace negative values (floor at 0)
SELECT greatest(price, 0) AS price FROM items

-- Complex condition with CASE
SELECT CASE WHEN price < 0 THEN 0 ELSE price END AS price FROM items
```

---

## 15. LowerStringProcessor

Convert strings to lowercase.

| | |
|---|---|
| **Honeysuckle** | [`lower_string_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/lower_string_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"email": ["John@Example.COM"]})

# Convert to lowercase
df = df.with_columns(
    pl.col("email").str.to_lowercase()
)
```

### DuckDB

```sql
-- Convert to lowercase (dot notation - preferred)
SELECT email.lower() AS email FROM items

-- Traditional function syntax
SELECT lower(email) AS email FROM items
```

---

## 16. KeepOnlyProcessor

Keep only specified columns, drop all others.

| | |
|---|---|
| **Honeysuckle** | [`keep_only_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/keep_only_processor.py) |
| **Polars** | [`select()`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.select.html) |
| **DuckDB** | Column list in SELECT |

```python
# Honeysuckle
KeepOnlyProcessor(column_names=["id", "title", "date"])
```

### Example

| id | title | author | date | | | id | title | date |
|----|-------|--------|------|---|---|----|-------|------|
| 1 | "Art" | "Jane" | "2024" | → | | 1 | "Art" | "2024" |

### Polars

```python
import polars as pl

# Input
df = pl.DataFrame({"id": [1], "title": ["Art"], "author": ["Jane"], "date": ["2024"]})

# Keep only specified columns
df = df.select(["id", "title", "date"])

# Or using pl.col
df = df.select(pl.col("id", "title", "date"))
```

### DuckDB

```sql
-- Keep only specified columns
SELECT id, title, date FROM items
```

---

## 17. DropNullColumnsProcessor

Drop columns where ALL values are null.

| | |
|---|---|
| **Honeysuckle** | [`drop_null_columns_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/drop_null_columns_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"id": [1, 2], "empty_col": [None, None], "title": ["Art", "Photo"]})

# Drop columns where all values are null
df = df.select([
    col for col in df.columns
    if not df[col].is_null().all()
])

# Or more idiomatic
null_cols = [col for col in df.columns if df[col].is_null().all()]
df = df.drop(null_cols)
```

### DuckDB

```sql
-- DuckDB doesn't have built-in "drop all-null columns"
-- You'd need to check columns individually or use dynamic SQL
-- In practice, select only the columns you need:
SELECT id, title FROM items
```

---

## 18. DropColumnsProcessor

Drop specified columns from the dataframe.

| | |
|---|---|
| **Honeysuckle** | [`drop_columns_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/drop_columns_processor.py) |
| **Polars** | [`drop()`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.drop.html) |
| **DuckDB** | [`EXCLUDE`](https://duckdb.org/docs/sql/expressions/star#exclude-clause) |

```python
# Honeysuckle
DropColumnsProcessor(column_names=["internal_id", "temp_field"])
```

### Example

| id | internal_id | title | | | id | title |
|----|-------------|-------|---|---|----|-------|
| 1 | "abc123" | "Art" | → | | 1 | "Art" |

### Polars

```python
import polars as pl

# Input
df = pl.DataFrame({"id": [1], "internal_id": ["abc123"], "title": ["Art"]})

# Drop specified columns
df = df.drop(["internal_id", "temp_field"])

# Drop single column
df = df.drop("internal_id")
```

### DuckDB

```sql
-- Drop columns using EXCLUDE (DuckDB-specific)
SELECT * EXCLUDE (internal_id, temp_field) FROM items

-- Or explicitly list columns to keep
SELECT id, title FROM items
```

---

## 19. CopyFieldDataframeProcessor

Copy values from one column to another (create new column).

| | |
|---|---|
| **Honeysuckle** | [`copy_field_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/copy_field_processor.py) |
| **Polars** | [`alias()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.alias.html) |
| **DuckDB** | `SELECT col AS new_col` |

```python
# Honeysuckle
CopyFieldDataframeProcessor(source_field="original_title", target_field="display_title")
```

### Example

| original_title | | | original_title | display_title |
|----------------|---|---|----------------|---------------|
| `"Mona Lisa"` | → | | `"Mona Lisa"` | `"Mona Lisa"` |

### Polars

```python
import polars as pl

# Input
df = pl.DataFrame({"original_title": ["Mona Lisa"]})

# Copy column to new name
df = df.with_columns(
    pl.col("original_title").alias("display_title")
)

# Copy multiple columns at once
df = df.with_columns(
    pl.col("title").alias("display_title"),
    pl.col("date").alias("publication_date"),
)
```

### DuckDB

```sql
-- Copy column
SELECT *, original_title AS display_title FROM items

-- Copy multiple
SELECT *, title AS display_title, date AS publication_date FROM items
```

---

## 20. ReplaceProcessor

Replace string values in a column.

| | |
|---|---|
| **Honeysuckle** | [`replace_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/replace_processor.py) |
| **Polars** | [`str.replace()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.replace.html) / [`str.replace_all()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.replace_all.html) |
| **DuckDB** | [`.replace()`](https://duckdb.org/docs/sql/functions/text#replacestring-source-target) (dot notation) |

```python
# Honeysuckle
ReplaceProcessor(column_name="text", old_value="&amp;", new_value="&")
ReplaceProcessor(column_name="phone", old_value="-", new_value="")
```

### Example

| text | | | text |
|------|---|---|------|
| `"Tom &amp; Jerry"` | → | | `"Tom & Jerry"` |

### Polars

```python
import polars as pl

# Input
df = pl.DataFrame({"text": ["Tom &amp; Jerry"]})

# Replace all occurrences
df = df.with_columns(
    pl.col("text").str.replace_all("&amp;", "&")
)

# Remove character
df = df.with_columns(
    pl.col("phone").str.replace_all("-", "")
)

# Regex replace
df = df.with_columns(
    pl.col("text").str.replace_all(r"\s+", " ")  # Collapse whitespace
)
```

### DuckDB

```sql
-- Replace all occurrences (dot notation - preferred)
SELECT text.replace('&amp;', '&') AS text FROM items

-- Remove character
SELECT phone.replace('-', '') AS phone FROM items

-- Regex replace
SELECT regexp_replace(text, '\s+', ' ', 'g') AS text FROM items
```

---

## 21. MergeProcessor

Join/merge two dataframes together.

| | |
|---|---|
| **Honeysuckle** | [`merge_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/merge_processor.py) |
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
| id | artist_id | artist_name |
|----|-----------|-------------|
| 1 | 100 | "Monet" |

### Polars

```python
import polars as pl

# Input
sales = pl.DataFrame({"id": [1], "artist_id": [100]})
artists = pl.DataFrame({"id": [100], "artist_name": ["Monet"]})

# Left join
df = sales.join(
    artists.select(["id", "artist_name"]),
    left_on="artist_id",
    right_on="id",
    how="left"
)

# Inner join
df = sales.join(artists, left_on="artist_id", right_on="id", how="inner")

# Join with suffix for duplicate column names
df = sales.join(artists, left_on="artist_id", right_on="id", suffix="_artist")
```

### DuckDB

```sql
-- Left join
SELECT s.*, a.artist_name
FROM sales s
LEFT JOIN artists a ON s.artist_id = a.id

-- Inner join
SELECT s.*, a.artist_name
FROM sales s
INNER JOIN artists a ON s.artist_id = a.id

-- Using USING for same column name
SELECT * FROM sales JOIN artists USING (artist_id)
```

---

## 22. FillMissingTuplesProcessor

Fill missing values in struct/tuple columns.

| | |
|---|---|
| **Honeysuckle** | [`fill_missing_tuples_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/fill_missing_tuples_processor.py) |
| **Polars** | [`struct.field()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.struct.field.html) with [`fill_null()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.fill_null.html) |
| **DuckDB** | [Struct access](https://duckdb.org/docs/sql/data_types/struct#retrieving-from-structs) with `coalesce()` |

```python
# Honeysuckle
FillMissingTuplesProcessor(column_name="dimensions", fill_value={"width": 0, "height": 0})
```

### Example

| dimensions | | | dimensions |
|------------|---|---|------------|
| `{width: null, height: 100}` | → | | `{width: 0, height: 100}` |

### Polars

```python
import polars as pl

# Rebuild struct with filled null fields
df = df.with_columns(
    pl.struct(
        pl.col("dimensions").struct.field("width").fill_null(0).alias("width"),
        pl.col("dimensions").struct.field("height").fill_null(0).alias("height"),
    ).alias("dimensions")
)
```

### DuckDB

```sql
-- Rebuild struct with coalesce on fields
SELECT {
    'width': coalesce(dimensions.width, 0),
    'height': coalesce(dimensions.height, 0)
} AS dimensions
FROM items

-- Fill entire null struct
SELECT coalesce(dimensions, {'width': 0, 'height': 0}) AS dimensions FROM items
```

---

## 23. ConditionalBoolProcessor

Evaluate a pandas query expression and set boolean result.

| | |
|---|---|
| **Honeysuckle** | [`conditional_bool_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/conditional_bool_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"price": [1500, 500], "status": ["active", "active"]})

# Evaluate expression
df = df.with_columns(
    ((pl.col("price") > 1000) & (pl.col("status") == "active")).alias("is_premium")
)

# Complex expressions
df = df.with_columns(
    (
        (pl.col("price") > 1000) |
        (pl.col("featured") == True)
    ).alias("is_highlight")
)
```

### DuckDB

```sql
-- Evaluate expression
SELECT *, (price > 1000 AND status = 'active') AS is_premium FROM items

-- Complex expressions
SELECT *, (price > 1000 OR featured = true) AS is_highlight FROM items
```

---

## 24. CapitalizeProcessor

Capitalize the first character of strings.

| | |
|---|---|
| **Honeysuckle** | [`capitalize_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/capitalize_processor.py) |
| **Polars** | [`str.to_titlecase()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.to_titlecase.html) (or custom) |
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
import polars as pl

# Input
df = pl.DataFrame({"title": ["hello world"]})

# Capitalize first char only (like Python's str.capitalize())
df = df.with_columns(
    (pl.col("title").str.slice(0, 1).str.to_uppercase() +
     pl.col("title").str.slice(1)).alias("title")
)

# Title case (capitalize each word)
df = df.with_columns(
    pl.col("title").str.to_titlecase()
)
```

### DuckDB

```sql
-- Capitalize first char of each word (title case)
SELECT initcap(title) AS title FROM items

-- Capitalize only first char (like Python capitalize)
SELECT upper(title[1]) || title[2:] AS title FROM items
```

---

## 25. AppendStringProcessor

Append or prepend a string to column values.

| | |
|---|---|
| **Honeysuckle** | [`append_string_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/append_string_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"id": ["123"], "filename": ["image"]})

# Prepend string
df = df.with_columns(
    (pl.lit("ID-") + pl.col("id")).alias("id")
)

# Append string
df = df.with_columns(
    (pl.col("filename") + pl.lit(".jpg")).alias("filename")
)

# Using concat_str
df = df.with_columns(
    pl.concat_str([pl.lit("ID-"), pl.col("id")]).alias("id")
)
```

### DuckDB

```sql
-- Prepend string
SELECT 'ID-' || id AS id FROM items

-- Append string
SELECT filename || '.jpg' AS filename FROM items

-- Using concat
SELECT concat('ID-', id) AS id FROM items
```

---

## 26. AddNumberProcessor

Add a numeric value to column values.

| | |
|---|---|
| **Honeysuckle** | [`add_number_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/add_number_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"year": [2024]})

# Add value to new column
df = df.with_columns(
    (pl.col("year") + 1).alias("next_year")
)

# Add to same column
df = df.with_columns(
    pl.col("price") + 10
)
```

### DuckDB

```sql
-- Add value to new column
SELECT *, year + 1 AS next_year FROM items

-- Add to computed expression
SELECT *, price + tax AS total FROM items
```

---

## 27. SubtractNumberProcessor

Subtract a numeric value from column values.

| | |
|---|---|
| **Honeysuckle** | [`subtract_number_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/subtract_number_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"year": [2024]})

# Subtract value
df = df.with_columns(
    (pl.col("year") - 1).alias("prev_year")
)

# Calculate difference
df = df.with_columns(
    (pl.col("sale_price") - pl.col("cost")).alias("profit")
)
```

### DuckDB

```sql
-- Subtract value
SELECT *, year - 1 AS prev_year FROM items

-- Calculate difference
SELECT *, sale_price - cost AS profit FROM items
```

---

## 28. SplitStringProcessor

Split string into a list based on delimiter.

| | |
|---|---|
| **Honeysuckle** | [`split_string_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/split_string_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"tags": ["a,b,c"]})

# Split string to list
df = df.with_columns(
    pl.col("tags").str.split(",")
)

# Split with default space delimiter
df = df.with_columns(
    pl.col("full_name").str.split(" ")
)
```

### DuckDB

```sql
-- Split string to list
SELECT string_split(tags, ',') AS tags FROM items

-- Split with default space
SELECT string_split(full_name, ' ') AS name_parts FROM items
```

---

## 29. AsTypeProcessor

Cast column to a different data type.

| | |
|---|---|
| **Honeysuckle** | [`as_type_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/as_type_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"year": ["2024"], "price": ["99.99"]})

# Cast types (strict=True raises on failure, strict=False returns null)
df = df.with_columns(
    pl.col("year").cast(pl.Int64),
    pl.col("price").cast(pl.Float64),
    pl.col("id").cast(pl.String),
)
```

### DuckDB

```sql
-- Cast using :: shorthand (preferred)
SELECT
    year::INTEGER AS year,
    price::DOUBLE AS price,
    id::VARCHAR AS id
FROM items

-- Alternative CAST syntax
SELECT CAST(year AS INTEGER) AS year FROM items
```

---

## 30. DropRowsOnConditionProcessor

Filter/drop rows based on a condition.

| | |
|---|---|
| **Honeysuckle** | [`drop_rows_on_condition_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/drop_rows_on_condition_processor.py) |
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
import polars as pl

# Input
df = pl.DataFrame({"status": ["active", "deleted"], "price": [100, 50]})

# Keep rows where condition is NOT met (drop where status == "deleted")
df = df.filter(pl.col("status") != "deleted")

# Keep rows where price >= 0 (drop where price < 0)
df = df.filter(pl.col("price") >= 0)

# Multiple conditions
df = df.filter(
    (pl.col("status") != "deleted") &
    (pl.col("price") >= 0)
)
```

### DuckDB

```sql
-- Keep rows (drop where status = 'deleted')
SELECT * FROM items WHERE status != 'deleted'

-- Keep rows where price >= 0
SELECT * FROM items WHERE price >= 0

-- Multiple conditions
SELECT * FROM items
WHERE status != 'deleted'
  AND price >= 0
```

---

## Quick Reference

| Processor | Polars | DuckDB |
|-----------|--------|--------|
| `FillEmptyProcessor` | `.fill_null()` | `coalesce()` / `ifnull()` |
| `RemoveMultivalueNullsProcessor` | `.list.eval(pl.element().drop_nulls())` | `list_filter(x -> x IS NOT NULL)` |
| `StripStringProcessor` | `.str.strip_chars()` | `.trim()` |
| `ExtractFirstProcessor` | `.list.first()` | `col[1]` |
| `ExtractOnConditionProcessor` | `pl.when().then().otherwise()` | `if()` / `CASE WHEN` |
| `ContainsBoolProcessor` | `.str.contains()` | `.contains()` |
| `StringConstantDataframeProcessor` | `pl.lit()` | `'value' AS col` |
| `AppendOnConditionProcessor` | `pl.when().then()` + `.list.concat()` | `list_concat()` + `CASE` |
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
| `ReplaceProcessor` | `.str.replace_all()` | `.replace()` |
| `MergeProcessor` | `.join()` | `JOIN` |
| `FillMissingTuplesProcessor` | `.struct.field().fill_null()` | `coalesce(struct.field)` |
| `ConditionalBoolProcessor` | Boolean expression | Boolean expression |
| `CapitalizeProcessor` | Slice + uppercase | `initcap()` |
| `AppendStringProcessor` | `+` / `pl.concat_str()` | `||` / `concat()` |
| `AddNumberProcessor` | `+` | `+` |
| `SubtractNumberProcessor` | `-` | `-` |
| `SplitStringProcessor` | `.str.split()` | `string_split()` |
| `AsTypeProcessor` | `.cast()` | `::TYPE` |
| `DropRowsOnConditionProcessor` | `.filter()` | `WHERE` |
