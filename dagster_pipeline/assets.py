import sys, os, datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dagster import (
    asset, define_asset_job, ScheduleDefinition, Definitions,
    RetryPolicy, asset_check, AssetCheckResult,
)
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

from extract import extract
from land import land
from load import load

PROJECT_ROOT = Path(__file__).parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "markets_dbt"
PROFILES_DIR = os.environ["DBT_PROFILES_DIR"]

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=PROFILES_DIR)
dbt_project.prepare_if_dev()


def today():
    return datetime.date.today().isoformat()


def snowflake_stats():
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        token=os.environ["SNOWFLAKE_PAT"],
        authenticator="PROGRAMMATIC_ACCESS_TOKEN",
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="RAW",
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*), MAX(trade_date) FROM RAW.DAILY_PRICES")
    row_count, latest_date = cur.fetchone()
    conn.close()
    return row_count, latest_date


@asset(retry_policy=RetryPolicy(max_retries=3, delay=30))
def raw_prices():
    """Pull daily prices from the API and save raw files."""
    day = today()
    extract(day)
    return day


@asset
def landed(raw_prices):
    """Upload the raw files to S3."""
    land(raw_prices)
    return raw_prices


@asset
def loaded(landed):
    """Flatten and load the raw data from S3 into Snowflake."""
    load(landed)
    return "loaded"


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@asset_check(asset=loaded)
def data_is_fresh(context):
    """Fail if the most recent trading day in Snowflake is too old."""
    _, latest_date = snowflake_stats()
    age_days = (datetime.date.today() - latest_date).days
    return AssetCheckResult(
        passed=(age_days <= 7),
        metadata={"latest_trade_date": str(latest_date), "age_days": age_days},
    )


@asset_check(asset=loaded)
def has_enough_rows(context):
    """Fail if the load produced no rows."""
    row_count, _ = snowflake_stats()
    return AssetCheckResult(
        passed=(row_count > 0),
        metadata={"row_count": row_count},
    )


daily_job = define_asset_job("daily_market_pipeline")
daily_schedule = ScheduleDefinition(job=daily_job, cron_schedule="30 22 * * 1-5")

defs = Definitions(
    assets=[raw_prices, landed, loaded, dbt_models],
    asset_checks=[data_is_fresh, has_enough_rows],
    jobs=[daily_job],
    schedules=[daily_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR, profiles_dir=PROFILES_DIR),
    },
)