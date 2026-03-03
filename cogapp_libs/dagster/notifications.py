"""Email notification helpers using Jinja2 templates.

Provides functions to render HTML email notifications for pipeline events:
- Pipeline success
- Pipeline failure
- Daily summary

Templates are stored in cogapp_libs/dagster/templates/email/ and use Jinja2 syntax.

Usage:
    from cogapp_libs.dagster.notifications import render_success_email, send_notification

    html = render_success_email(
        pipeline_name="polars_pipeline",
        completed_at="2024-01-15 10:30:00",
        duration="2m 15s",
        assets_materialized=5,
        assets=[
            {"name": "sales_transform_polars", "records": 1234},
            {"name": "artworks_transform_polars", "records": 567},
        ],
    )

    send_notification(
        to=["team@example.com"],
        subject="Pipeline Success: polars_pipeline",
        html=html,
    )
"""

from __future__ import annotations

import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

from dagster import RunsFilter
from jinja2 import Environment, FileSystemLoader, select_autoescape

# Template directory
TEMPLATE_DIR = Path(__file__).parent / "templates" / "email"

# Cogapp brand colours (from cogapp.com _tokens.scss)
# Injected as Jinja2 globals so templates stay free of hardcoded hex values.
BRAND_COLORS = {
    "slate": "#282828",  # primary dark grey — text, headings, buttons
    "cream": "#ebebe1",  # light neutral — page bg, card bg, borders
    "grey": "#717171",  # mid-tone — secondary text, labels, footer
    "white": "#ffffff",
    "black": "#000000",
    "focus_blue": "#227bff",  # interactive / links
    # Semantic status colours (kept for success/failure distinction)
    "success": "#22c55e",
    "success_dark": "#16a34a",
    "success_bg": "#f0fdf4",
    "success_border": "#bbf7d0",
    "success_pill_bg": "#dcfce7",
    "success_pill_text": "#166534",
    "failure": "#ef4444",
    "failure_dark": "#dc2626",
    "failure_bg": "#fef2f2",
    "failure_border": "#fecaca",
    "failure_pill_bg": "#fee2e2",
    "failure_pill_text": "#991b1b",
    "failure_deep": "#7f1d1d",
    "warning_asset_bg": "#fff7ed",
    "warning_asset_border": "#fed7aa",
    "warning_asset_text": "#c2410c",
    "stacktrace_text": "#f3f4f6",
}


def _get_jinja_env() -> Environment:
    """Create Jinja2 environment with template directory and brand colours."""
    env = Environment(
        loader=FileSystemLoader(TEMPLATE_DIR),
        autoescape=select_autoescape(["html", "html.j2", "xml"]),
    )
    env.globals["c"] = BRAND_COLORS
    return env


def render_success_email(
    pipeline_name: str,
    completed_at: str | datetime,
    duration: str,
    assets_materialized: int,
    assets: list[dict[str, Any]] | None = None,
    total_records: int | str | None = None,
    checks_passed: int | None = None,
    checks_total: int | None = None,
    dagster_url: str | None = None,
    run_id: str | None = None,
    environment: str = "production",
    internal: bool = True,
) -> str:
    """Render pipeline success email template.

    Args:
        pipeline_name: Name of the pipeline
        completed_at: Completion timestamp
        duration: Human-readable duration (e.g., "2m 15s")
        assets_materialized: Number of assets materialized
        assets: List of asset dicts with 'name' and optional 'records'
        total_records: Total records processed across all assets
        checks_passed: Number of asset checks that passed
        checks_total: Total number of checks run
        dagster_url: URL to Dagster UI for this run
        run_id: Dagster run ID
        environment: Environment name (production, staging, etc.)
        internal: If True, include Dagster links, run IDs, environment.
            Set False for client-facing emails.

    Returns:
        Rendered HTML string
    """
    env = _get_jinja_env()
    template = env.get_template("pipeline_success.html.j2")

    if isinstance(completed_at, datetime):
        completed_at = completed_at.strftime("%Y-%m-%d %H:%M:%S")

    return template.render(
        pipeline_name=pipeline_name,
        completed_at=completed_at,
        duration=duration,
        assets_materialized=assets_materialized,
        assets=assets or [],
        total_records=total_records,
        checks_passed=checks_passed,
        checks_total=checks_total,
        dagster_url=dagster_url,
        run_id=run_id,
        environment=environment,
        internal=internal,
    )


def render_failure_email(
    pipeline_name: str,
    failed_at: str | datetime,
    error_message: str,
    failed_asset: str | None = None,
    failed_step: str | None = None,
    stacktrace: str | None = None,
    assets_succeeded: int = 0,
    assets_failed: int = 1,
    duration: str | None = None,
    dagster_url: str | None = None,
    logs_url: str | None = None,
    run_id: str | None = None,
    environment: str = "production",
    retry_count: int | None = None,
    tags: dict[str, str] | None = None,
    internal: bool = True,
) -> str:
    """Render pipeline failure email template.

    Args:
        pipeline_name: Name of the pipeline
        failed_at: Failure timestamp
        error_message: Error message
        failed_asset: Name of the asset that failed
        failed_step: Step within the asset that failed
        stacktrace: Full stack trace
        assets_succeeded: Number of assets that succeeded before failure
        assets_failed: Number of assets that failed
        duration: Time until failure
        dagster_url: URL to Dagster UI for this run
        logs_url: URL to logs
        run_id: Dagster run ID
        environment: Environment name
        retry_count: Number of retry attempts
        tags: Dagster run tags (e.g. schedule name, partition key)
        internal: If True, include Dagster links, run IDs, stacktrace, tags.
            Set False for client-facing emails.

    Returns:
        Rendered HTML string
    """
    env = _get_jinja_env()
    template = env.get_template("pipeline_failure.html.j2")

    if isinstance(failed_at, datetime):
        failed_at = failed_at.strftime("%Y-%m-%d %H:%M:%S")

    return template.render(
        pipeline_name=pipeline_name,
        failed_at=failed_at,
        error_message=error_message,
        failed_asset=failed_asset,
        failed_step=failed_step,
        stacktrace=stacktrace,
        assets_succeeded=assets_succeeded,
        assets_failed=assets_failed,
        duration=duration,
        dagster_url=dagster_url,
        logs_url=logs_url,
        run_id=run_id,
        environment=environment,
        retry_count=retry_count,
        tags=tags,
        internal=internal,
    )


def render_daily_summary(
    date: str,
    total_runs: int,
    successful_runs: int,
    failed_runs: int,
    runs: list[dict[str, Any]] | None = None,
    metrics: dict[str, Any] | None = None,
    skipped_runs: int = 0,
    dagster_url: str | None = None,
    environment: str = "production",
    generated_at: str | datetime | None = None,
    internal: bool = True,
) -> str:
    """Render daily summary email template.

    Args:
        date: Date of the summary (e.g., "2024-01-15")
        total_runs: Total number of pipeline runs
        successful_runs: Number of successful runs
        failed_runs: Number of failed runs
        runs: List of run dicts with 'name', 'success', 'duration', 'records'
        metrics: Dict with 'total_records', 'avg_duration', 'success_rate'
        skipped_runs: Number of skipped runs
        dagster_url: URL to Dagster UI
        environment: Environment name
        generated_at: When the summary was generated
        internal: If True, include Dagster links and environment.
            Set False for client-facing emails.

    Returns:
        Rendered HTML string
    """
    env = _get_jinja_env()
    template = env.get_template("daily_summary.html.j2")

    if generated_at is None:
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(generated_at, datetime):
        generated_at = generated_at.strftime("%Y-%m-%d %H:%M:%S")

    return template.render(
        date=date,
        total_runs=total_runs,
        successful_runs=successful_runs,
        failed_runs=failed_runs,
        runs=runs or [],
        metrics=metrics,
        skipped_runs=skipped_runs,
        dagster_url=dagster_url,
        environment=environment,
        generated_at=generated_at,
        internal=internal,
    )


def send_notification(
    to: list[str],
    subject: str,
    html: str,
    from_addr: str = "pipeline@example.com",
    smtp_host: str = "localhost",
    smtp_port: int = 587,
    smtp_user: str | None = None,
    smtp_password: str | None = None,
    use_tls: bool = True,
) -> None:
    """Send HTML email notification.

    Args:
        to: List of recipient email addresses
        subject: Email subject
        html: HTML body content
        from_addr: Sender email address
        smtp_host: SMTP server hostname
        smtp_port: SMTP server port
        smtp_user: SMTP username (optional)
        smtp_password: SMTP password (optional)
        use_tls: Whether to use TLS
    """
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to)

    # Attach HTML content
    msg.attach(MIMEText(html, "html"))

    # Send email
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        if use_tls:
            server.starttls()
        if smtp_user and smtp_password:
            server.login(smtp_user, smtp_password)
        server.sendmail(from_addr, to, msg.as_string())


# -----------------------------------------------------------------------------
# Dagster Sensor Integration
# -----------------------------------------------------------------------------

# Dagster internal tag prefixes — not useful in email notifications
_HIDDEN_TAG_PREFIXES = (".", "dagster/")


def _extract_run_tags(run: Any) -> dict[str, str] | None:
    """Extract user-visible tags from a Dagster run, dropping internal ones."""
    if not run.tags:
        return None
    visible = {k: v for k, v in run.tags.items() if not k.startswith(_HIDDEN_TAG_PREFIXES)}
    return visible or None


def create_success_sensor_handler(
    recipients: list[str],
    smtp_config: dict[str, Any] | None = None,
    dagster_url_base: str | None = None,
    environment: str = "production",
    internal: bool = True,
) -> Any:
    """Create a handler function for Dagster run success sensors.

    Extracts asset materialization count, duration, and record counts
    from the completed run to populate the success email template.

    Usage:
        from dagster import run_status_sensor, DagsterRunStatus

        @run_status_sensor(run_status=DagsterRunStatus.SUCCESS)
        def email_on_success(context):
            handler = create_success_sensor_handler(
                recipients=["team@example.com"],
                dagster_url_base="https://dagster.example.com",
            )
            handler(context)
    """

    def handler(context: Any) -> None:
        """Handle run success event."""
        run = context.dagster_run

        dagster_url = None
        if dagster_url_base:
            dagster_url = f"{dagster_url_base}/runs/{run.run_id}"

        # Extract materialization info from the run log
        records = context.instance.get_records_for_run(run_id=run.run_id).records
        assets: list[dict[str, Any]] = []
        total_records = 0
        for record in records:
            event = record.event_log_entry.dagster_event
            if event and event.is_step_materialization:
                mat = event.step_materialization_data.materialization
                name = mat.asset_key.to_user_string()
                row_count = None
                if mat.metadata:
                    for key in ("record_count", "row_count", "dagster/row_count"):
                        if key in mat.metadata:
                            row_count = mat.metadata[key].value
                            break
                assets.append({"name": name, "records": row_count})
                if row_count and isinstance(row_count, (int, float)):
                    total_records += int(row_count)

        # Extract asset check results from the run log
        checks_passed = 0
        checks_total = 0
        for record in records:
            event = record.event_log_entry.dagster_event
            if event and event.event_type_value == "ASSET_CHECK_EVALUATION":
                checks_total += 1
                check_eval = event.asset_check_evaluation_data
                if check_eval and check_eval.passed:
                    checks_passed += 1

        # Compute duration from run record timestamps
        # (start_time/end_time live on RunRecord, not DagsterRun)
        duration = "N/A"
        run_records = context.instance.get_run_records(
            filters=RunsFilter(run_ids=[run.run_id]),
            limit=1,
        )
        if run_records:
            record = run_records[0]
            if record.start_time and record.end_time:
                elapsed = int(record.end_time - record.start_time)
                minutes, seconds = divmod(elapsed, 60)
                duration = f"{minutes}m {seconds}s" if minutes else f"{seconds}s"

        html = render_success_email(
            pipeline_name=run.job_name,
            completed_at=datetime.now(),
            duration=duration,
            assets_materialized=len(assets),
            assets=assets or None,
            total_records=total_records or None,
            checks_passed=checks_passed if checks_total else None,
            checks_total=checks_total if checks_total else None,
            run_id=run.run_id,
            dagster_url=dagster_url,
            environment=environment,
            internal=internal,
        )

        smtp_config_final = smtp_config or {}
        send_notification(
            to=recipients,
            subject=f"[SUCCESS] Pipeline: {run.job_name}",
            html=html,
            **smtp_config_final,
        )

    return handler


def create_failure_sensor_handler(
    recipients: list[str],
    smtp_config: dict[str, Any] | None = None,
    dagster_url_base: str | None = None,
    environment: str = "production",
    internal: bool = True,
) -> Any:
    """Create a handler function for Dagster run failure sensors.

    Usage:
        from dagster import run_failure_sensor

        @run_failure_sensor
        def email_on_failure(context):
            handler = create_failure_sensor_handler(
                recipients=["team@example.com"],
                dagster_url_base="https://dagster.example.com",
            )
            handler(context)
    """

    def handler(context: Any) -> None:
        """Handle run failure event."""
        run = context.dagster_run
        error = context.failure_event.message if context.failure_event else "Unknown error"

        dagster_url = None
        if dagster_url_base:
            dagster_url = f"{dagster_url_base}/runs/{run.run_id}"

        html = render_failure_email(
            pipeline_name=run.job_name,
            failed_at=datetime.now(),
            error_message=error,
            run_id=run.run_id,
            dagster_url=dagster_url,
            environment=environment,
            tags=_extract_run_tags(run),
            internal=internal,
        )

        smtp_config_final = smtp_config or {}
        send_notification(
            to=recipients,
            subject=f"[FAILED] Pipeline: {run.job_name}",
            html=html,
            **smtp_config_final,
        )

    return handler


__all__ = [
    "create_failure_sensor_handler",
    "create_success_sensor_handler",
    "render_daily_summary",
    "render_failure_email",
    "render_success_email",
    "send_notification",
]
