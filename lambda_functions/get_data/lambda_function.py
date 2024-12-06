# ================================================================================
# BIBLIOTECA
# ================================================================================

import os
from datetime import datetime, timedelta
from io import BytesIO

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from gridstatusio import GridStatusClient
from zoneinfo import ZoneInfo

# ================================================================================
# CONSTANTES
# ================================================================================

# Número máx de valores por query
QUERY_LIMIT = 6

# Nome do bucket
BUCKET = os.getenv("S3_BUCKET")

# Chave API Grid Status
GRIDSTATUS_API_KEY = os.getenv("GRIDSTATUS_API_KEY")

# ================================================================================
# FUNÇÕES
# ================================================================================


def s3_file_exists(bucket: str, file_key: str) -> bool:
    s3_client = boto3.client("s3")

    try:
        s3_client.head_object(Bucket=bucket, Key=file_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise Exception(e)


def save_on_s3(
    bucket: str,
    s3_file_path: str,
    data_buffer: BytesIO,
) -> None:
    """
    Uploads a file from an in-memory BytesIO buffer to S3.
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket)

    # Reset buffer position to the beginning
    data_buffer.seek(0)

    # Upload data to S3
    bucket.put_object(Key=s3_file_path, Body=data_buffer)

    return None


def handler(event, context):

    now = datetime.now(tz=ZoneInfo("UTC"))

    if now.minute // 30 > 0:
        time_str = now.strftime(format="%Y-%m-%dT%H")
        file_name = f'{now.strftime(format="%Y_%m_%d-%H")}h00.parquet'
        start = f"{time_str}:00"
        end = f"{time_str}:30"
    else:
        time_str = now.strftime(format="%Y-%m-%dT%H")
        end = f"{time_str}:00"
        now = now - timedelta(hours=1)
        file_name = f'{now.strftime(format="%Y_%m_%d-%H")}h30.parquet'
        time_str = now.strftime(format="%Y-%m-%dT%H")
        start = f"{time_str}:30"

    s3_file_path = f"energy_grid/{file_name}"

    if s3_file_exists(bucket=BUCKET, file_key=s3_file_path):
        print("Consulta já feita!")
        return "Consulta já feita!"

    # Fetch the data
    print("Fetching dataset from GridStatusClient...")
    print(f"> Start date: {start}\n> End date: {end}")
    grid_client = GridStatusClient(api_key=GRIDSTATUS_API_KEY)
    data = grid_client.get_dataset(
        dataset="caiso_fuel_mix",
        start=start,
        end=end,
        tz="UTC",
        limit=QUERY_LIMIT,
    )
    print("Dataset fetched!")

    # Parse dates
    data["interval_start_utc"] = pd.to_datetime(data["interval_start_utc"])
    data["interval_end_utc"] = pd.to_datetime(data["interval_end_utc"])

    # Write parquet to an in-memory buffer
    data_buffer = BytesIO()
    print("data.to_parquet ...")
    data.to_parquet(data_buffer, index=False)
    print("data.to_parquet success!")

    # Upload to S3
    print("save_on_s3 ...")
    save_on_s3(
        bucket=BUCKET,
        s3_file_path=s3_file_path,
        data_buffer=data_buffer,
    )
    print("save_on_s3 success!")

    return "Deu bom!"
