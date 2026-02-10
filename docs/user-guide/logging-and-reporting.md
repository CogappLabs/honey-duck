---
title: Logging & Reporting
description: Asset logging, run failure handling, metadata-driven observability, and notification patterns for Dagster OSS.
---

# Logging & Reporting

How to log within assets, react to failures, collect run metadata, and send notifications in Dagster OSS.

!!! info "Dagster OSS vs Dagster+"
    Dagster+ (paid) includes built-in alert policies, email/Slack/PagerDuty integrations configured via UI, and SLA monitoring. In OSS, **sensors are your alerting layer** - everything else (metadata, context.log, asset checks) works identically.

## Asset-Level Logging

Every asset receives a `context` object with a built-in logger. All output is captured per-run and visible in the Dagster UI run logs. See [Dagster Logging](https://docs.dagster.io/guides/log-debug/logging) for full details.

```python
@dg.asset
def sales_transform(context: dg.AssetExecutionContext, paths: PathsResource) -> pl.DataFrame:
    context.log.info(f"Loading data from {paths.harvest_dir}")
    result = load_and_transform(paths)

    context.log.info(f"Transformed {len(result):,} records")
    return result
```

### Log Levels

| Level | Use for | Example |
|-------|---------|---------|
| `context.log.debug()` | Verbose internals | Column lists, SQL queries |
| `context.log.info()` | Progress and results | Row counts, timing, file paths |
| `context.log.warning()` | Non-fatal issues | Missing optional config, empty results |
| `context.log.error()` | Errors you handle | Caught exceptions before re-raising |

!!! tip "Enable debug logging"
    ```bash
    export DAGSTER_CLI_LOG_LEVEL=DEBUG
    uv run dg dev
    ```

### Timing

Use `time.perf_counter()` to measure and log elapsed time:

```python
import time

@dg.asset
def sales_transform(context: dg.AssetExecutionContext) -> pl.DataFrame:
    start = time.perf_counter()
    result = load_and_transform()
    elapsed_ms = (time.perf_counter() - start) * 1000

    context.log.info(f"Completed transformation in {elapsed_ms:.1f}ms")
    context.add_output_metadata({"processing_time_ms": round(elapsed_ms, 2)})

    return result
```

!!! tip "Reusable timing"
    For repeated use, wrap this in a context manager. See `track_timing` in the [Dagster Helpers API](../api/dagster-helpers.md) for a ready-made version.

---

## Metadata-Driven Observability

Dagster's primary observability mechanism is **structured metadata** on asset materializations, not log lines. Metadata appears in the UI asset catalog and can be queried via GraphQL. See [Asset Metadata and Tags](https://docs.dagster.io/guides/build/assets/metadata-and-tags) and the [MetadataValue API](https://docs.dagster.io/api/dagster/metadata).

### Adding Metadata to Assets

```python
@dg.asset
def sales_transform(context: dg.AssetExecutionContext) -> pl.DataFrame:
    result = transform()

    context.add_output_metadata({
        "record_count": len(result),
        "columns": result.columns,
        "high_value_count": len(result.filter(pl.col("price") > 50_000)),
    })

    return result
```

### Metadata Types

Dagster renders metadata differently based on type. Simple Python values (int, float, str) are auto-converted. Use `MetadataValue` for rich types:

```python
context.add_output_metadata({
    # Simple values (auto-converted)
    "record_count": 1500,
    "status": "success",
    "processing_time_ms": 42.5,

    # Rich types
    "preview": dg.MetadataValue.md(str(df.head(5))),
    "output_path": dg.MetadataValue.path("/data/output/sales.json"),
    "dagster_url": dg.MetadataValue.url("http://localhost:3000/runs/abc123"),
    "schema": dg.MetadataValue.json({"columns": df.columns}),
})
```

### Table Previews in Metadata

To show a data preview table in the Dagster UI, use `MetadataValue.md()` with Polars' built-in [markdown formatting](https://docs.pola.rs/api/python/stable/reference/api/polars.Config.set_tbl_formatting.html):

```python
# Polars DataFrame preview as markdown table
with pl.Config(tbl_formatting="ASCII_MARKDOWN", tbl_hide_dataframe_shape=True):
    preview_md = str(df.head(5))

context.add_output_metadata({
    "preview": dg.MetadataValue.md(preview_md),
})
```

For a richer table with column types and schema info, use [Table Metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags/table-metadata):

```python
context.add_output_metadata({
    "schema": dg.MetadataValue.table_schema(
        dg.TableSchema(
            columns=[
                dg.TableColumn(name=col, type=str(dtype))
                for col, dtype in zip(df.columns, df.dtypes)
            ]
        )
    ),
})
```

!!! abstract "Helper opportunity: `df_preview_metadata`"
    The markdown preview + schema metadata pattern appears in most assets. A helper could accept a DataFrame and return a metadata dict with both:

    ```python
    def df_preview_metadata(
        df: pl.DataFrame, rows: int = 5, title: str = "preview",
    ) -> dict[str, dg.MetadataValue]:
        with pl.Config(tbl_formatting="ASCII_MARKDOWN", tbl_hide_dataframe_shape=True):
            preview_md = str(df.head(rows))
        return {
            title: dg.MetadataValue.md(preview_md),
            "record_count": len(df),
            "columns": dg.MetadataValue.json(df.columns),
            "schema": dg.MetadataValue.table_schema(
                dg.TableSchema(
                    columns=[
                        dg.TableColumn(name=col, type=str(dtype))
                        for col, dtype in zip(df.columns, df.dtypes)
                    ]
                )
            ),
        }

    # Usage: context.add_output_metadata(df_preview_metadata(result))
    ```

---

## Handling Run Failures

### Run Failure Sensor

The recommended way to react to failures in Dagster OSS. Fires whenever any job run fails. See [Run Status Sensors](https://docs.dagster.io/guides/automate/sensors/run-status-sensors).

```python
@dg.run_failure_sensor(monitor_all_code_locations=True)
def on_run_failure(context: dg.RunFailureSensorContext):
    """Alert on any job failure across all code locations."""
    run = context.dagster_run
    error = context.failure_event.message

    context.log.info(
        f"Run {run.run_id} for job '{run.job_name}' failed: {error}"
    )

    # Send notification (implement send_alert with your preferred method)
    send_alert(
        subject=f"[Dagster] {run.job_name} failed",
        body=f"Run {run.run_id} failed:\n{error}",
    )
```

Scope to specific jobs:

```python
from honey_duck.defs.shared.jobs import polars_pipeline, duckdb_pipeline

@dg.run_failure_sensor(monitored_jobs=[polars_pipeline, duckdb_pipeline])
def on_pipeline_failure(context: dg.RunFailureSensorContext):
    ...
```

### Blocking Asset Checks

Asset checks with `blocking=True` prevent downstream assets from materializing when validation fails. This is the first line of defence - catch bad data before it propagates. See [Asset Checks](https://docs.dagster.io/guides/test/data-contracts) and the [API reference](https://docs.dagster.io/api/dagster/asset-checks).

```python
@dg.asset_check(asset=sales_transform, blocking=True)
def check_sales_schema(sales_transform: pl.DataFrame) -> dg.AssetCheckResult:
    """Block downstream if schema is invalid."""
    try:
        SalesSchema.validate(sales_transform)
        return dg.AssetCheckResult(passed=True)
    except pa.errors.SchemaError as e:
        return dg.AssetCheckResult(passed=False, metadata={"error": str(e)})
```

!!! abstract "Helper opportunity: `pandera_asset_check`"
    The try/validate/catch pattern is identical for every Pandera-based blocking check. A factory could generate the check from a schema class:

    ```python
    def pandera_asset_check(
        asset: dg.AssetsDefinition,
        schema: type[pa.DataFrameModel],
        blocking: bool = True,
    ) -> dg.AssetChecksDefinition:
        @dg.asset_check(asset=asset, blocking=blocking)
        def _check(context, **kwargs) -> dg.AssetCheckResult:
            df = next(iter(kwargs.values()))
            try:
                schema.validate(df)
                return dg.AssetCheckResult(
                    passed=True,
                    metadata={"record_count": len(df), "schema": schema.__name__},
                )
            except pa.errors.SchemaError as e:
                return dg.AssetCheckResult(
                    passed=False,
                    metadata={"error": str(e), "schema": schema.__name__},
                )
        _check.__name__ = f"check_{asset.key.to_python_identifier()}_schema"
        return _check

    # Usage:
    check_sales = pandera_asset_check(sales_transform, SalesTransformSchema)
    check_artworks = pandera_asset_check(artworks_transform, ArtworksTransformSchema)
    ```

See [Pandera Validation](../integrations/pandera-validation.md) for schema definitions and [Soda Validation](../integrations/soda-validation.md) for SQL-based contract checks.

---

## Success Reports with Metadata

### Run Status Sensor

For emailing a full report when a run succeeds, use `@run_status_sensor` to collect all materialization metadata:

```python
@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[polars_pipeline, duckdb_pipeline],
)
def on_run_success(context: dg.RunStatusSensorContext):
    """Email a report with all asset metadata on success."""
    run = context.dagster_run
    instance = context.instance

    # Get materialization events from the completed run
    records = instance.get_records_for_run(run_id=run.run_id).records

    report_lines = []
    for record in records:
        event = record.event_log_entry
        if event.dagster_event and event.dagster_event.is_step_materialization:
            mat = event.dagster_event.step_materialization_data.materialization
            asset_key = mat.asset_key.to_user_string()
            metadata = {m.label: m.value for m in mat.metadata}
            report_lines.append(f"  {asset_key}: {metadata}")

    send_report(
        subject=f"[Dagster] {run.job_name} completed",
        body=f"Run {run.run_id} materialized {len(report_lines)} assets:\n"
             + "\n".join(report_lines),
    )
```

!!! abstract "Helper opportunity: `collect_run_metadata`"
    The event-loop-and-filter pattern for extracting materializations from a run is verbose and reused across sensors. A helper could return a clean list of `{asset_key, metadata}` dicts:

    ```python
    def collect_run_metadata(
        instance: dg.DagsterInstance, run_id: str,
    ) -> list[dict]:
        """Extract asset materializations and their metadata from a completed run."""
        records = instance.get_records_for_run(run_id=run_id).records
        results = []
        for record in records:
            event = record.event_log_entry
            if event.dagster_event and event.dagster_event.is_step_materialization:
                mat = event.dagster_event.step_materialization_data.materialization
                results.append({
                    "asset_key": mat.asset_key.to_user_string(),
                    "metadata": {m.label: m.value for m in mat.metadata},
                })
        return results

    # Usage in sensor:
    assets = collect_run_metadata(context.instance, run.run_id)
    ```

    The same pattern applies to collecting asset check results (see Pattern 3 below).

### Available Run Statuses

| Status | Trigger |
|--------|---------|
| `DagsterRunStatus.SUCCESS` | Run completed without errors |
| `DagsterRunStatus.FAILURE` | Run failed (prefer `@run_failure_sensor` for this) |
| `DagsterRunStatus.CANCELED` | Run was manually canceled |
| `DagsterRunStatus.STARTED` | Run began executing |

---

## Notification Assets

A notification asset is a regular Dagster asset that depends on pipeline outputs and sends a message when they complete. See [Defining Assets](https://docs.dagster.io/guides/build/assets/defining-assets) for asset fundamentals.

### Slack Notifications

```python
import os
import requests

@dg.asset(
    deps=["sales_output", "artworks_output"],
    kinds={"slack", "notification"},
    group_name="notifications",
)
def notify_sales_complete(context) -> None:
    """Send Slack notification after pipeline completes."""
    webhook_url = os.environ["SLACK_WEBHOOK_URL"]

    response = requests.post(
        webhook_url,
        json={"text": "Sales pipeline completed successfully"},
        timeout=10,
    )
    response.raise_for_status()
    context.log.info("Slack notification sent")
```

### Email Notifications

```python
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

@dg.asset(
    deps=["sales_output", "artworks_output"],
    kinds={"email", "notification"},
    group_name="notifications",
)
def email_pipeline_report(context) -> None:
    """Send email report after pipeline completes."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Daily Pipeline Report"
    msg["From"] = os.environ["SMTP_USER"]
    msg["To"] = "team@example.com"
    msg.attach(MIMEText("Pipeline completed successfully.", "plain"))

    with smtplib.SMTP(os.environ["SMTP_HOST"], int(os.environ["SMTP_PORT"])) as server:
        server.starttls()
        server.login(os.environ["SMTP_USER"], os.environ["SMTP_PASSWORD"])
        server.send_message(msg)

    context.log.info("Email report sent")
```

!!! abstract "Helper opportunity: `SmtpResource`"
    The SMTP connection boilerplate (env vars, starttls, login) is repeated for every email-sending asset or sensor. A Dagster `ConfigurableResource` would centralise this:

    ```python
    class SmtpResource(dg.ConfigurableResource):
        host: str
        port: int = 587
        user: str
        password: str

        def send(self, to: str | list[str], subject: str, body: str, html: str | None = None):
            recipients = [to] if isinstance(to, str) else to
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.user
            msg["To"] = ", ".join(recipients)
            msg.attach(MIMEText(body, "plain"))
            if html:
                msg.attach(MIMEText(html, "html"))
            with smtplib.SMTP(self.host, self.port) as server:
                server.starttls()
                server.login(self.user, self.password)
                server.send_message(msg)

    # In definitions.py:
    defs = dg.Definitions(
        resources={"smtp": SmtpResource(
            host=dg.EnvVar("SMTP_HOST"),
            port=587,
            user=dg.EnvVar("SMTP_USER"),
            password=dg.EnvVar("SMTP_PASSWORD"),
        )},
    )

    # In any asset:
    @dg.asset(deps=["sales_output"])
    def email_report(context, smtp: SmtpResource):
        smtp.send(to="team@example.com", subject="Report", body="Done")
    ```

Configure SMTP via environment variables:

| Variable | Description |
|----------|-------------|
| `SMTP_HOST` | SMTP server hostname |
| `SMTP_PORT` | SMTP server port |
| `SMTP_USER` | SMTP username |
| `SMTP_PASSWORD` | SMTP password |

### Notification Assets vs Sensors

| Approach | When to use |
|----------|-------------|
| **Notification assets** | Trigger after specific assets in the same job (declared as `deps`) |
| **Run failure sensor** | React to any job failure across all jobs/code locations |
| **Run status sensor** | React to any status change, collect cross-asset metadata for reports |

Notification assets are simpler but only fire within a single job run. Sensors are more flexible and can monitor across jobs.

---

## Data Quality Warnings

Not all validation issues should stop the pipeline. Source systems often have messy data that needs flagging without blocking downstream processing. This project uses [Pandera](../integrations/pandera-validation.md) for schema validation with helpers in `shared/schemas.py`.

### Lazy Validation

Standard Pandera validation fails on the first error. [Lazy validation](https://pandera.readthedocs.io/en/stable/lazy_validation.html) collects **all** errors before returning. Use `validate_lazy()` to get a complete picture:

```python
from honey_duck.defs.shared.schemas import validate_lazy, SalesTransformSchema

result = validate_lazy(df, SalesTransformSchema)

if not result.valid:
    print(f"Found {result.error_count} issues:")
    for err in result.errors:
        print(f"  Column '{err['column']}': {err['check']} — {err['failure_case']}")
```

`ValidationResult` contains:

| Field | Type | Description |
|-------|------|-------------|
| `valid` | `bool` | Whether all rows passed |
| `error_count` | `int` | Total number of errors |
| `errors` | `list[dict]` | Each error's column, check name, and failure case |
| `invalid_indices` | `set[int]` | Row indices that failed |

### Pattern 1: Filter Bad Rows and Log Warnings

Use `filter_invalid_rows()` to remove invalid rows and continue processing. Attach the warnings as asset metadata so they're visible in the Dagster UI and available to sensors:

```python
from honey_duck.defs.shared.schemas import (
    filter_invalid_rows,
    validate_lazy,
    SalesTransformSchema,
)

@dg.asset
def sales_transform(context: dg.AssetExecutionContext) -> pl.DataFrame:
    result = transform()

    # Filter out bad rows, keep the rest
    clean_df, removed = filter_invalid_rows(result, SalesTransformSchema)

    if removed > 0:
        # Collect the actual errors for reporting
        validation = validate_lazy(result, SalesTransformSchema)
        context.log.warning(f"Filtered {removed} invalid rows ({validation.error_count} errors)")
        context.add_output_metadata({
            "validation_warnings": dg.MetadataValue.json({
                "removed_rows": removed,
                "error_count": validation.error_count,
                "errors": validation.errors,
            }),
        })

    return clean_df
```

!!! abstract "Helper opportunity: `validate_and_filter`"
    Pattern 1 calls `filter_invalid_rows` then `validate_lazy` separately to get both the clean DataFrame and error details. A combined helper could do both in one pass:

    ```python
    def validate_and_filter(
        context: dg.AssetExecutionContext,
        df: pl.DataFrame,
        schema: type[pa.DataFrameModel],
    ) -> pl.DataFrame:
        """Filter invalid rows and attach validation warnings as metadata."""
        clean_df, removed = filter_invalid_rows(df, schema)
        if removed > 0:
            result = validate_lazy(df, schema)
            context.log.warning(f"Filtered {removed} invalid rows ({result.error_count} errors)")
            context.add_output_metadata({
                "validation_warnings": dg.MetadataValue.json({
                    "removed_rows": removed,
                    "error_count": result.error_count,
                    "errors": result.errors,
                }),
            })
        return clean_df

    # Usage: clean_df = validate_and_filter(context, result, SalesTransformSchema)
    ```

### Pattern 2: Non-Blocking Asset Check

Create asset checks without `blocking=True`. The pipeline continues regardless of the result, but failures are visible in the UI and queryable by sensors:

```python
@dg.asset_check(asset=sales_transform)  # No blocking=True
def check_sales_data_quality(sales_transform: pl.DataFrame) -> dg.AssetCheckResult:
    """Warn about data quality issues without stopping the pipeline."""
    result = validate_lazy(sales_transform, SalesTransformSchema)

    return dg.AssetCheckResult(
        passed=result.valid,
        metadata={
            "error_count": result.error_count,
            "errors": dg.MetadataValue.json(result.errors) if result.errors else None,
            "invalid_row_count": len(result.invalid_indices),
        },
    )
```

### Pattern 3: Sensor That Emails Failed Check Results

Combine non-blocking checks with a run status sensor that collects warnings after a successful run and emails them as an attachment:

```python
@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[polars_pipeline, duckdb_pipeline],
)
def email_validation_warnings(context: dg.RunStatusSensorContext):
    """After a successful run, check for non-blocking check failures and email a report."""
    records = context.instance.get_records_for_run(
        run_id=context.dagster_run.run_id,
    ).records

    warnings = []
    for record in records:
        event = record.event_log_entry
        if not event.dagster_event:
            continue

        # Collect failed non-blocking asset checks
        if event.dagster_event.is_asset_check_evaluation:
            check_eval = event.dagster_event.asset_check_evaluation_data
            if not check_eval.passed:
                warnings.append({
                    "check": check_eval.check_name,
                    "asset": check_eval.asset_key.to_user_string(),
                    "metadata": {
                        m.label: str(m.value)
                        for m in (check_eval.metadata or {})
                    },
                })

        # Also collect validation_warnings from asset metadata
        if event.dagster_event.is_step_materialization:
            mat = event.dagster_event.step_materialization_data.materialization
            for entry in mat.metadata:
                if entry.label == "validation_warnings":
                    warnings.append({
                        "asset": mat.asset_key.to_user_string(),
                        "metadata": entry.value,
                    })

    if not warnings:
        return  # No warnings, no email

    # Write warnings file and email
    import json
    from pathlib import Path

    warnings_path = Path("data/output/validation_warnings.json")
    warnings_path.write_text(json.dumps(warnings, indent=2))

    send_warnings_email(
        subject=f"[Dagster] {len(warnings)} validation warnings in {context.dagster_run.job_name}",
        body=f"Run {context.dagster_run.run_id} completed with {len(warnings)} data quality warnings.",
        attachment_path=warnings_path,
    )
```

### Pattern 4: Dedicated Warnings Report Asset

For pipelines where you always want a warnings file generated, create a dedicated asset that validates all transforms and produces a report:

```python
@dg.asset(
    deps=["sales_transform", "artworks_transform"],
    kinds={"validation", "report"},
    group_name="quality",
)
def validation_warnings_report(context, paths: PathsResource) -> dict:
    """Validate all transforms and produce a warnings report."""
    import json

    all_warnings = []

    for name, schema in [
        ("sales_transform", SalesTransformSchema),
        ("artworks_transform", ArtworksTransformSchema),
    ]:
        df = pl.read_parquet(paths.storage_dir / f"{name}.parquet")
        result = validate_lazy(df, schema)

        if not result.valid:
            for err in result.errors:
                err["asset"] = name
                all_warnings.append(err)

    if not all_warnings:
        context.log.info("All transforms passed validation")
        context.add_output_metadata({"warning_count": 0})
        return {"status": "clean", "warning_count": 0}

    # Write warnings file
    warnings_path = Path(paths.output_dir) / "validation_warnings.json"
    warnings_path.write_text(json.dumps(all_warnings, indent=2, default=str))

    context.log.warning(f"Found {len(all_warnings)} validation warnings")
    context.add_output_metadata({
        "warning_count": len(all_warnings),
        "warnings_file": dg.MetadataValue.path(str(warnings_path)),
        "warnings_preview": dg.MetadataValue.json(all_warnings[:10]),
    })

    return {"status": "warnings", "warning_count": len(all_warnings)}
```

### Choosing an Approach

| Pattern | Pros | Cons |
|---------|------|------|
| **Filter + log** | Simplest, no extra assets | Warnings only in metadata/logs |
| **Non-blocking check** | Visible in UI checks tab | No automatic notification |
| **Sensor + email** | Automatic alerts, cross-job | More complex, runs after completion |
| **Dedicated asset** | Produces a file, can be a dependency | Adds to the asset graph |

For most teams: start with **non-blocking checks** (Pattern 2) for visibility, then add the **sensor** (Pattern 3) when you want email alerts. Use the **dedicated asset** (Pattern 4) if downstream consumers need the warnings file.

---

## Recommended Architecture

```
Asset check fails (blocking)      -->  Prevents downstream materialization
Asset check fails (non-blocking)  -->  Visible in UI, picked up by sensor
Data quality warnings              -->  validate_lazy() metadata + sensor email
Run fails                          -->  @run_failure_sensor  -->  Slack/email alert
Run succeeds                       -->  @run_status_sensor   -->  Email metadata report
Job completes                      -->  Notification asset   -->  Slack channel post
```

### Registering Sensors

Add sensors to your Definitions:

```python
# In definitions.py
defs = dg.Definitions(
    assets=[...],
    sensors=[on_run_failure, on_run_success],
    # ...
)
```

Sensors evaluate on a polling interval (default 30 seconds). Set a custom interval:

```python
@dg.run_failure_sensor(
    monitor_all_code_locations=True,
    minimum_interval_seconds=60,
)
def on_run_failure(context):
    ...
```

---

## What's Not Available in OSS

These features require [Dagster+](https://dagster.io/plus):

- **UI-configured alert policies** (email/Slack/PagerDuty without code)
- **Freshness policy alerts** (automatic alerts when assets go stale)
- **SLA monitoring** with built-in dashboards
- **Insights** (historical performance metrics and trends)

In OSS, you build equivalent functionality with sensors and metadata. The patterns above cover the most common needs.
