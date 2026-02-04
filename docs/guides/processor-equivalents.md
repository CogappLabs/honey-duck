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

## Top 15 Processors by Usage

Based on analysis of Huntington Extractor and Universal Yiddish Library codebases.

| Rank | Processor | Usage | Operation |
|------|-----------|-------|-----------|
| 1 | `FillEmptyProcessor` | 69 | Fill nulls with column/constant |
| 2 | `RemoveMultivalueNullsProcessor` | 59 | Remove nulls from lists |
| 3 | `ExtractFirstProcessor` | 46 | Get first element from list |
| 4 | `StripStringProcessor` | 40 | Strip whitespace |
| 5 | `ContainsBoolProcessor` | 34 | Check if string contains pattern |
| 6 | `CopyFieldDataframeProcessor` | 30 | Copy column values |
| 7 | `ExtractOnConditionProcessor` | 24 | Extract value conditionally |
| 8 | `ReplaceProcessor` | 21 | String replacement |
| 9 | `StringConstantDataframeProcessor` | 20 | Add constant column |
| 10 | `ReplaceOnConditionProcessor` | 20 | Conditional value replacement |
| 11 | `FillMissingTuplesProcessor` | 18 | Fill missing tuple values |
| 12 | `AsTypeProcessor` | 17 | Cast column types |
| 13 | `ImplodeProcessor` | 17 | Aggregate values into lists |
| 14 | `DropRowsOnConditionProcessor` | 14 | Filter rows by condition |
| 15 | `ExplodeColumnsProcessor` | 14 | Unnest lists to rows |

---

## 1. FillEmptyProcessor

Fill empty values with values from a different column or a constant.

| | |
|---|---|
| **Honeysuckle** | [`fill_empty_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/fill_empty_processor.py) |
| **Polars** | [`fill_null()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.fill_null.html) |
| **DuckDB** | [`coalesce()`](https://duckdb.org/docs/sql/functions/utility#coabordalesceval1-val2-) / [`ifnull()`](https://duckdb.org/docs/sql/functions/utility#ifnullexpr-other) |

```python
# Honeysuckle
FillEmptyProcessor(column_name="title", column_fill="alt_title")
FillEmptyProcessor(column_name="price", constant_fill=0)
FillEmptyProcessor(column_name="name", column_fill="display_name", constant_fill="Unknown")
```

### Polars

```python
import polars as pl

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

### Polars

```python
import polars as pl

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

## 3. ExtractFirstProcessor

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

### Polars

```python
import polars as pl

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

## 4. StripStringProcessor

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

### Polars

```python
import polars as pl

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

## 5. ContainsBoolProcessor

Check if a string contains a pattern and set boolean result.

| | |
|---|---|
| **Honeysuckle** | [`contains_bool_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/contains_bool_processor.py) |
| **Polars** | [`str.contains()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.contains.html) |
| **DuckDB** | [`.contains()`](https://duckdb.org/docs/sql/functions/text#containsstring-search_string) (dot notation) or [`LIKE`/`ILIKE`](https://duckdb.org/docs/sql/functions/pattern_matching) |

```python
# Honeysuckle
ContainsBoolProcessor(column_name="description", result_column="has_keyword", pattern="important")
ContainsBoolProcessor(column_name="email", result_column="is_gmail", pattern=r"@gmail\.com$")
```

### Polars

```python
import polars as pl

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

## 6. CopyFieldDataframeProcessor

Copy values from one column to another.

| | |
|---|---|
| **Honeysuckle** | [`copy_field_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/copy_field_processor.py) |
| **Polars** | [`alias()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.alias.html) |
| **DuckDB** | `SELECT col AS new_col` |

```python
# Honeysuckle
CopyFieldDataframeProcessor(source_field="original_title", target_field="display_title")
```

### Polars

```python
import polars as pl

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

## 7. ExtractOnConditionProcessor

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

### Polars

```python
import polars as pl

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

## 8. ReplaceProcessor

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

### Polars

```python
import polars as pl

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

## 9. StringConstantDataframeProcessor

Add a column with a constant string value.

| | |
|---|---|
| **Honeysuckle** | [`string_constant_dataframe_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/string_constant_dataframe_processor.py) |
| **Polars** | [`pl.lit()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.lit.html) |
| **DuckDB** | Literal value in SELECT |

```python
# Honeysuckle
StringConstantDataframeProcessor(column_name="source", value="huntington")
StringConstantDataframeProcessor(column_name="version", value="2.0")
```

### Polars

```python
import polars as pl

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

## 10. ReplaceOnConditionProcessor

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

### Polars

```python
import polars as pl

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

## 11. FillMissingTuplesProcessor

Fill missing values in tuple/struct columns.

| | |
|---|---|
| **Honeysuckle** | [`fill_missing_tuples_processor.py`](https://github.com/Cogapp/honeysuckle/blob/main/honeysuckle/components/processors/fill_missing_tuples_processor.py) |
| **Polars** | [`struct.field()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.struct.field.html) with [`fill_null()`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.fill_null.html) |
| **DuckDB** | [Struct access](https://duckdb.org/docs/sql/data_types/struct#retrieving-from-structs) with `coalesce()` |

```python
# Honeysuckle
FillMissingTuplesProcessor(column_name="dimensions", fill_value={"width": 0, "height": 0})
```

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

## 12. AsTypeProcessor

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
AsTypeProcessor(column_name="id", new_type="str")
```

### Polars

```python
import polars as pl

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

## 13. ImplodeProcessor

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

### Polars

```python
import polars as pl

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

## 14. DropRowsOnConditionProcessor

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

### Polars

```python
import polars as pl

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

## 15. ExplodeColumnsProcessor

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

### Polars

```python
import polars as pl

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

## Quick Reference

| Processor | Polars | DuckDB |
|-----------|--------|--------|
| `FillEmptyProcessor` | `.fill_null()` | `coalesce()` / `ifnull()` |
| `RemoveMultivalueNullsProcessor` | `.list.eval(pl.element().drop_nulls())` | `list_filter(x -> x IS NOT NULL)` |
| `ExtractFirstProcessor` | `.list.first()` | `col[1]` |
| `StripStringProcessor` | `.str.strip_chars()` | `.trim()` |
| `ContainsBoolProcessor` | `.str.contains()` | `.contains()` |
| `CopyFieldDataframeProcessor` | `.alias()` | `AS new_col` |
| `ExtractOnConditionProcessor` | `pl.when().then().otherwise()` | `if()` / `CASE WHEN` |
| `ReplaceProcessor` | `.str.replace_all()` | `.replace()` |
| `StringConstantDataframeProcessor` | `pl.lit()` | `'value' AS col` |
| `ReplaceOnConditionProcessor` | `pl.when().then().otherwise()` | `if()` / `greatest()` |
| `FillMissingTuplesProcessor` | `.struct.field().fill_null()` | `coalesce(struct.field)` |
| `AsTypeProcessor` | `.cast()` | `::TYPE` |
| `ImplodeProcessor` | `.group_by().agg()` | `list()` + `GROUP BY ALL` |
| `DropRowsOnConditionProcessor` | `.filter()` | `WHERE` |
| `ExplodeColumnsProcessor` | `.explode()` | `unnest()` |
