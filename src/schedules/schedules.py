"""Dagster schedules for ELT pipeline."""

from dagster import ScheduleDefinition
from src.jobs.elt_jobs import full_elt_job


# Daily schedule at 06:00 Europe/Berlin
daily_elt_schedule = ScheduleDefinition(
    name="daily_elt_schedule",
    job=full_elt_job,
    cron_schedule="0 6 * * *",
    execution_timezone="Europe/Berlin",
    description="Run full ELT pipeline daily at 06:00 Europe/Berlin",
)
