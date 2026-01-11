"""Example notification assets for honey-duck pipeline.

This module demonstrates how to use the notification asset patterns from cogapp_deps.

To enable notifications:
1. Set environment variables for Slack/email configuration
2. Add notification assets to definitions.py
3. Notifications will trigger after pipeline completion

Environment Variables for Slack:
- SLACK_WEBHOOK_URL: Slack webhook URL for posting messages

Environment Variables for Email:
- SMTP_HOST: SMTP server hostname (e.g., smtp.gmail.com)
- SMTP_PORT: SMTP server port (e.g., 587)
- SMTP_USER: SMTP username/email
- SMTP_PASSWORD: SMTP password or app-specific password

Example .env configuration:
    SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
    SMTP_HOST=smtp.gmail.com
    SMTP_PORT=587
    SMTP_USER=your-email@example.com
    SMTP_PASSWORD=your-app-password
"""

from cogapp_deps.dagster import (
    create_email_notification_asset,
    create_pipeline_status_notification,
    create_slack_notification_asset,
)

# Example 1: Slack notification after original pipeline completes
notify_pipeline_complete = create_slack_notification_asset(
    name="notify_pipeline_complete",
    deps=["sales_output", "artworks_output"],
    webhook_url_env_var="SLACK_WEBHOOK_URL",
    message_template="✅ Honey Duck pipeline completed successfully! Processed {asset_count} output assets.",
    channel="#data-pipeline-alerts",
)

# Example 2: Slack notification for Polars pipeline
notify_polars_pipeline = create_slack_notification_asset(
    name="notify_polars_pipeline",
    deps=["sales_output_polars", "artworks_output_polars"],
    webhook_url_env_var="SLACK_WEBHOOK_URL",
    message_template="✅ Polars pipeline completed with {asset_count} assets.",
    channel="#data-pipeline-alerts",
)

# Example 3: Email notification with custom template
notify_pipeline_email = create_email_notification_asset(
    name="notify_pipeline_email",
    deps=["sales_output", "artworks_output"],
    recipient_emails=["data-team@example.com", "manager@example.com"],
    subject_template="Daily Honey Duck Pipeline Report - {asset_count} assets completed",
    body_template="""
Pipeline Completion Report
==========================

The honey-duck data pipeline has completed successfully.

Assets materialized: {asset_count}
Timestamp: {timestamp}

Check the Dagster UI for detailed metrics and logs:
http://localhost:3000

---
This is an automated notification from the Honey Duck data pipeline.
    """,
)

# Example 4: Using the convenience wrapper
notify_duckdb_pipeline = create_pipeline_status_notification(
    name="notify_duckdb_pipeline",
    deps=["sales_output_duckdb", "artworks_output_duckdb"],
    notification_type="slack",
    webhook_url_env_var="SLACK_WEBHOOK_URL",
    channel="#data-pipeline-alerts",
)

# Example 5: Multiple recipients with email
notify_stakeholders = create_email_notification_asset(
    name="notify_stakeholders",
    deps=["sales_output", "artworks_output"],
    recipient_emails=[
        "cto@example.com",
        "data-lead@example.com",
        "analytics-team@example.com",
    ],
    subject_template="Honey Duck Pipeline Report - {asset_count} assets",
    body_template="Pipeline completed at {timestamp}. All {asset_count} assets materialized successfully.",
)


# To use these in your pipeline, add them to definitions.py:
"""
from honey_duck.defs.notifications_example import (
    notify_pipeline_complete,
    notify_pipeline_email,
)

defs = dg.Definitions(
    assets=[
        # ... existing assets
        notify_pipeline_complete,
        notify_pipeline_email,
    ],
    # ... rest of definitions
)
"""
