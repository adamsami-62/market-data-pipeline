"""
land.py - Upload the raw JSON files from data/raw/ to the S3 bucket, 
organized under dated keys
"""

import os, glob
import boto3
from dotenv import load_dotenv

load_dotenv()

def land(run_date):
    """Upload all files in data/raw/ to the S3 bucket under dated keys."""
    bucket = os.environ["S3_BUCKET"]
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-2"            
    )

    # find all of today's raw files
    pattern = f"data/raw/*_{run_date}.json"
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No files found matching {pattern}")
    for local_path in files:
        filename = os.path.basename(local_path)
        key = f"raw/daily_prices/{run_date}/{filename}"
        s3.upload_file(local_path, bucket, key)
        print(f"landed s3://{bucket}/{key}")

if __name__ == "__main__":
    import datetime
    land(datetime.date.today().isoformat())


   