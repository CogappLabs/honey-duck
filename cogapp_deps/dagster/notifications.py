"""Notification asset patterns for Dagster pipelines.

Provides placeholder implementations for Slack and email notifications.
These are templates showing how to structure notification assets that depend
on pipeline completion.

Example usage:
    >>> from cogapp_deps.dagster.notifications import create_slack_notification_asset
    >>>
    >>> # Create a notification asset that depends on pipeline completion
    >>> notify_on_success = create_slack_notification_asset(
    ...     name="notify_pipeline_success",
    ...     deps=["sales_output", "artworks_output"],
    ...     webhook_url_env_var="SLACK_WEBHOOK_URL",
    ...     message_template="Pipeline completed: {asset_count} assets materialized",
    ... )
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import dagster as dg
from dagster import AssetExecutionContext

if TYPE_CHECKING:
    from collections.abc import Sequence


def create_slack_notification_asset(
    name: str,
    deps: Sequence[str],
    webhook_url_env_var: str = "SLACK_WEBHOOK_URL",
    message_template: str = "Pipeline completed successfully",
    channel: str | None = None,
    group_name: str = "notifications",
) -> dg.AssetsDefinition:
    """Create a Slack notification asset that triggers after dependencies complete.

    This is a placeholder implementation showing the pattern. In production, you would:
    1. Use @dg.resource to create a SlackResource with the webhook URL
    2. Use requests or slack_sdk to send actual messages
    3. Add retry logic and error handling

    Args:
        name: Asset name (e.g., "notify_pipeline_success")
        deps: Asset keys that must complete before notification
        webhook_url_env_var: Environment variable name for Slack webhook URL
        message_template: Message to send (can include {asset_count} placeholder)
        channel: Optional Slack channel to post to
        group_name: Dagster asset group name

    Returns:
        Asset definition for the notification

    Example:
        >>> notify = create_slack_notification_asset(
        ...     name="notify_sales_complete",
        ...     deps=["sales_output", "artworks_output"],
        ...     message_template="Sales pipeline completed: {asset_count} assets",
        ...     channel="#data-pipeline-alerts",
        ... )
    """

    @dg.asset(
        name=name,
        deps=deps,
        group_name=group_name,
        kinds={"slack", "notification"},
    )
    def slack_notification(context) -> dict:
        """Send Slack notification after dependencies complete.

        PLACEHOLDER IMPLEMENTATION - Replace with actual Slack API calls.

        Args:
            context: AssetExecutionContext provided by Dagster
        """
        import os

        webhook_url = os.getenv(webhook_url_env_var)

        if not webhook_url:
            context.log.warning(
                f"Slack webhook URL not configured (env var: {webhook_url_env_var}). "
                "Skipping notification."
            )
            return {
                "status": "skipped",
                "reason": "webhook_url_not_configured",
            }

        # Format message
        message = message_template.format(asset_count=len(deps))
        if channel:
            message = f"{channel}: {message}"

        # PLACEHOLDER: In production, use requests or slack_sdk here
        # Example:
        # import requests
        # response = requests.post(
        #     webhook_url,
        #     json={"text": message},
        #     timeout=10,
        # )
        # response.raise_for_status()

        context.log.info(f"[PLACEHOLDER] Would send Slack notification: {message}")
        context.log.info(f"[PLACEHOLDER] Webhook URL env var: {webhook_url_env_var}")
        context.log.info(f"[PLACEHOLDER] Dependencies completed: {', '.join(deps)}")

        context.add_output_metadata({
            "message": message,
            "channel": channel or "default",
            "dependencies": ", ".join(deps),
            "webhook_configured": bool(webhook_url),
        })

        return {
            "status": "sent",
            "message": message,
            "channel": channel,
            "dependencies": list(deps),
        }

    return slack_notification


def create_email_notification_asset(
    name: str,
    deps: Sequence[str],
    recipient_emails: list[str] | str,
    subject_template: str = "Pipeline Completion Notification",
    body_template: str = "Pipeline completed successfully. {asset_count} assets materialized.",
    smtp_config_env_prefix: str = "SMTP",
    group_name: str = "notifications",
) -> dg.AssetsDefinition:
    """Create an email notification asset that triggers after dependencies complete.

    This is a placeholder implementation showing the pattern. In production, you would:
    1. Use @dg.resource to create an EmailResource with SMTP config
    2. Use smtplib or a service like SendGrid/AWS SES
    3. Add HTML email templates and error handling

    Args:
        name: Asset name (e.g., "email_pipeline_report")
        deps: Asset keys that must complete before notification
        recipient_emails: Email address(es) to send to (string or list)
        subject_template: Email subject (can include {asset_count} placeholder)
        body_template: Email body (can include {asset_count} placeholder)
        smtp_config_env_prefix: Prefix for SMTP env vars (HOST, PORT, USER, PASSWORD)
        group_name: Dagster asset group name

    Returns:
        Asset definition for the notification

    Example:
        >>> notify = create_email_notification_asset(
        ...     name="email_pipeline_report",
        ...     deps=["sales_output", "artworks_output"],
        ...     recipient_emails=["team@example.com", "manager@example.com"],
        ...     subject_template="Daily Pipeline Report - {asset_count} assets",
        ...     body_template="Pipeline completed at {timestamp}. Check Dagster UI for details.",
        ... )
    """

    @dg.asset(
        name=name,
        deps=deps,
        group_name=group_name,
        kinds={"email", "notification"},
    )
    def email_notification(context) -> dict:
        """Send email notification after dependencies complete.

        PLACEHOLDER IMPLEMENTATION - Replace with actual email sending.

        Args:
            context: AssetExecutionContext provided by Dagster
        """
        import os
        from datetime import datetime

        # Parse recipients
        recipients = recipient_emails if isinstance(recipient_emails, list) else [recipient_emails]

        # Check SMTP configuration
        smtp_host = os.getenv(f"{smtp_config_env_prefix}_HOST")
        smtp_port = os.getenv(f"{smtp_config_env_prefix}_PORT")
        smtp_user = os.getenv(f"{smtp_config_env_prefix}_USER")
        smtp_password = os.getenv(f"{smtp_config_env_prefix}_PASSWORD")

        if not all([smtp_host, smtp_port, smtp_user, smtp_password]):
            context.log.warning(
                f"SMTP not fully configured (env vars: {smtp_config_env_prefix}_*). "
                "Skipping email notification."
            )
            return {
                "status": "skipped",
                "reason": "smtp_not_configured",
            }

        # Format message
        timestamp = datetime.now().isoformat()
        subject = subject_template.format(asset_count=len(deps), timestamp=timestamp)
        body = body_template.format(asset_count=len(deps), timestamp=timestamp)

        # PLACEHOLDER: In production, use smtplib or email service here
        # Example:
        # import smtplib
        # from email.mime.text import MIMEText
        #
        # msg = MIMEText(body)
        # msg["Subject"] = subject
        # msg["From"] = smtp_user
        # msg["To"] = ", ".join(recipients)
        #
        # with smtplib.SMTP(smtp_host, int(smtp_port)) as server:
        #     server.starttls()
        #     server.login(smtp_user, smtp_password)
        #     server.send_message(msg)

        context.log.info(f"[PLACEHOLDER] Would send email notification")
        context.log.info(f"[PLACEHOLDER] To: {', '.join(recipients)}")
        context.log.info(f"[PLACEHOLDER] Subject: {subject}")
        context.log.info(f"[PLACEHOLDER] Body: {body}")
        context.log.info(f"[PLACEHOLDER] Dependencies completed: {', '.join(deps)}")

        context.add_output_metadata({
            "subject": subject,
            "recipients": ", ".join(recipients),
            "dependencies": ", ".join(deps),
            "smtp_configured": bool(smtp_host and smtp_user),
        })

        return {
            "status": "sent",
            "subject": subject,
            "recipients": recipients,
            "dependencies": list(deps),
            "timestamp": timestamp,
        }

    return email_notification


def create_pipeline_status_notification(
    name: str,
    deps: Sequence[str],
    notification_type: str = "slack",
    **kwargs,
) -> dg.AssetsDefinition:
    """Create a notification asset with automatic status detection.

    This is a convenience wrapper that creates either Slack or email notifications
    with smart defaults based on pipeline completion.

    Args:
        name: Asset name
        deps: Asset keys to monitor
        notification_type: "slack" or "email"
        **kwargs: Additional arguments passed to specific notification creator

    Returns:
        Asset definition for the notification

    Example:
        >>> notify = create_pipeline_status_notification(
        ...     name="daily_pipeline_alert",
        ...     deps=["sales_output", "artworks_output"],
        ...     notification_type="slack",
        ...     webhook_url_env_var="SLACK_WEBHOOK_URL",
        ... )
    """
    if notification_type == "slack":
        return create_slack_notification_asset(
            name=name,
            deps=deps,
            message_template=kwargs.get(
                "message_template",
                f"✅ Pipeline '{name}' completed successfully with {{asset_count}} assets",
            ),
            **{k: v for k, v in kwargs.items() if k != "message_template"},
        )
    elif notification_type == "email":
        return create_email_notification_asset(
            name=name,
            deps=deps,
            recipient_emails=kwargs.get("recipient_emails", []),
            subject_template=kwargs.get(
                "subject_template",
                f"Pipeline '{name}' Completion Report",
            ),
            **{k: v for k, v in kwargs.items() if k not in ["recipient_emails", "subject_template"]},
        )
    else:
        raise ValueError(f"Unknown notification_type: {notification_type}. Use 'slack' or 'email'.")


__all__ = [
    "create_slack_notification_asset",
    "create_email_notification_asset",
    "create_pipeline_status_notification",
]
