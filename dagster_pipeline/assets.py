"""
assets.py — Dagster pipeline for the market-data flow.

Python assets:  raw_prices -> landed -> loaded   (extract -> S3 -> Snowflake)
dbt assets:     the dbt models (staging + marts) run after loaded.
Runs on a weekday schedule with retries on the extract step.
"""

import sys, os, datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dagster import asset, define_asset_job, ScheduleDefinition, Definitions, RetryPolicy
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

from extract import extract
from land import land
from load import load

PROJECT_ROOT = Path(__file__).parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "markets_dbt"
PROFILES_DIR = os.environ["DBT_PROFILES_DIR"]

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=PROFILES_DIR,
)
dbt_project.prepare_if_dev()


def today():
    return datetime.date.today().isoformat()


@asset(retry_policy=RetryPolicy(max_retries=3, delay=30))
def raw_prices():
    """Pull daily prices from the API and save raw files. Retries on failure."""
    day = today()
    extract(day)
    return day


@asset
def landed(raw_prices):
    """Upload the raw files to S3. Runs after raw_prices."""
    land(raw_prices)
    return raw_prices


@asset
def loaded(landed):
    """Flatten and load the raw data into Snowflake RAW.DAILY_PRICES. Runs after landed."""
    load(landed)
    return "loaded"


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


daily_job = define_asset_job("daily_market_pipeline")
daily_schedule = ScheduleDefinition(
    job=daily_job,
    cron_schedule="30 22 * * 1-5",
)

defs = Definitions(
    assets=[raw_prices, landed, loaded, dbt_models],
    jobs=[daily_job],
    schedules=[daily_schedule],
    resources={
        "dbt": DbtCliResource(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=PROFILES_DIR,
        )
    },
)