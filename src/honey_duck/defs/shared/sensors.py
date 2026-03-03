"""Email notification sensors for pipeline success and failure.

Both sensors inject NotificationResource and early-return when notifications
are disabled (recipients is blank), so they no-op in local dev.
"""

import dagster as dg
from dagster import DagsterRunStatus

from cogapp_libs.dagster.notifications import (
    create_failure_sensor_handler,
    create_success_sensor_handler,
)

from .resources import NotificationResource


@dg.run_failure_sensor(monitor_all_code_locations=True)
def email_on_failure(
    context: dg.RunFailureSensorContext,
    notification: NotificationResource,
) -> None:
    """Send email notification when any pipeline fails."""
    if not notification.enabled:
        return

    handler = create_failure_sensor_handler(
        recipients=notification.recipients_list,
        smtp_config=notification.smtp_config,
        dagster_url_base=notification.dagster_url or None,
        environment=notification.environment,
    )
    handler(context)


@dg.run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitor_all_code_locations=True,
)
def email_on_success(
    context: dg.RunStatusSensorContext,
    notification: NotificationResource,
) -> None:
    """Send email notification when any pipeline succeeds."""
    if not notification.enabled:
        return

    handler = create_success_sensor_handler(
        recipients=notification.recipients_list,
        smtp_config=notification.smtp_config,
        dagster_url_base=notification.dagster_url or None,
        environment=notification.environment,
    )
    handler(context)
