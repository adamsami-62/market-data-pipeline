import os, json
import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()


def flatten(raw, ticker):
    """Flatten one ticker's nested daily-prices JSON into a list of row dicts."""
    series = raw["Time Series (Daily)"]
    rows = []
    for date, vals in series.items():
        rows.append({
            "TICKER": ticker,
            "TRADE_DATE": date,
            "OPEN":  float(vals["1. open"]),
            "HIGH":  float(vals["2. high"]),
            "LOW":   float(vals["3. low"]),
            "CLOSE": float(vals["4. close"]),
            "VOLUME": int(vals["5. volume"]),
        })
    return rows


def read_from_s3(run_date):
    """Read all of run_date's raw files from S3 and flatten them into rows."""
    bucket = os.environ["S3_BUCKET"]
    prefix = f"raw/daily_prices/{run_date}/"
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-2",
    )

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:
        raise FileNotFoundError(f"No files in s3://{bucket}/{prefix}")

    rows = []
    for obj in response["Contents"]:
        filename = obj["Key"].split("/")[-1]      # e.g. AAPL_2026-10-05.json
        ticker = filename.split("_")[0]
        body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
        rows += flatten(json.loads(body), ticker)
    return rows


def load(run_date):
    """Load run_date's raw prices from S3 into Snowflake, replacing existing rows per ticker."""
    rows = read_from_s3(run_date)
    df = pd.DataFrame(rows)

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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS RAW.DAILY_PRICES (
            TICKER STRING, TRADE_DATE DATE,
            OPEN FLOAT, HIGH FLOAT, LOW FLOAT, CLOSE FLOAT, VOLUME NUMBER
        )
    """)

    # replace each ticker's rows rather than appending, so re-runs don't duplicate
    tickers = list(df["TICKER"].unique())
    placeholders = ",".join(["%s"] * len(tickers))
    cur.execute(f"DELETE FROM RAW.DAILY_PRICES WHERE TICKER IN ({placeholders})", tickers)

    write_pandas(conn, df, "DAILY_PRICES", quote_identifiers=False)
    conn.close()
    print(f"loaded {len(df)} rows for {len(tickers)} tickers from S3")


if __name__ == "__main__":
    import datetime
    load(datetime.date.today().isoformat())