"""Email notification helpers using Jinja2 templates.

Provides functions to render HTML email notifications for pipeline events:
- Pipeline success
- Pipeline failure
- Daily summary

Templates are stored in honey_duck/templates/ and use Jinja2 syntax.

Usage:
    from honey_duck.notifications import render_success_email, send_notification

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

from jinja2 import Environment, FileSystemLoader, select_autoescape

# Template directory
TEMPLATE_DIR = Path(__file__).parent / "templates"


def _get_jinja_env() -> Environment:
    """Create Jinja2 environment with template directory."""
    return Environment(
        loader=FileSystemLoader(TEMPLATE_DIR),
        autoescape=select_autoescape(["html", "xml"]),
    )


def render_success_email(
    pipeline_name: str,
    completed_at: str | datetime,
    duration: str,
    assets_materialized: int,
    assets: list[dict[str, Any]] | None = None,
    total_records: int | str | None = None,
    checks_passed: int | None = None,
    dagster_url: str | None = None,
    run_id: str | None = None,
    environment: str = "production",
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
        dagster_url: URL to Dagster UI for this run
        run_id: Dagster run ID
        environment: Environment name (production, staging, etc.)

    Returns:
        Rendered HTML string
    """
    env = _get_jinja_env()
    template = env.get_template("pipeline_success.html")

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
        dagster_url=dagster_url,
        run_id=run_id,
        environment=environment,
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

    Returns:
        Rendered HTML string
    """
    env = _get_jinja_env()
    template = env.get_template("pipeline_failure.html")

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

    Returns:
        Rendered HTML string
    """
    env = _get_jinja_env()
    template = env.get_template("daily_summary.html")

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


def create_failure_sensor_handler(
    recipients: list[str],
    smtp_config: dict[str, Any] | None = None,
    dagster_url_base: str | None = None,
    environment: str = "production",
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
        )

        smtp_config_final = smtp_config or {}
        send_notification(
            to=recipients,
            subject=f"[FAILED] Pipeline: {run.job_name}",
            html=html,
            **smtp_config_final,
        )

    return handler
