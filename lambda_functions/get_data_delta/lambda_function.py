# ================================================================================
# BIBLIOTECA
# ================================================================================

import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
from deltalake.writer import write_deltalake
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

# Infos AWS

# ================================================================================
# FUNÇÕES
# ================================================================================


def already_done():
    s3 = boto3.client("s3")
    bucket_name = "alecrimtechchallengetresbronze"
    file_key = "your_file_key"

    response = s3.head_object(Bucket=bucket_name, Key=file_key)
    last_modified = response['LastModified']

    now = datetime.now(tz=ZoneInfo("UTC"))

    if (now - last_modified) > timedelta(minutes=30):
        print("The file is more than 30 minutes old.")
        return False
    else:
        print("The file is less than 30 minutes old.")
        return True


def handler(event, context):

    now = datetime.now(tz=ZoneInfo("UTC"))

    if now.minute // 30 > 0:
        time_str = now.strftime(format="%Y-%m-%dT%H")
        start = f"{time_str}:00"
        end = f"{time_str}:30"
    else:
        time_str = now.strftime(format="%Y-%m-%dT%H")
        end = f"{time_str}:00"
        now = now - timedelta(hours=1)
        time_str = now.strftime(format="%Y-%m-%dT%H")
        start = f"{time_str}:30"

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

    # Criando coluna para dar "Partition By"
    data["year_month"] = data["interval_start_utc"].dt.strftime("%Y-%m")

    print("save_on_s3 ...")
    aws_config = {
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }
    write_deltalake(
        "s3://alecrimtechchallengetresbronze/energy_grid_api/",
        data,
        description="Tabela extraída da API pública através do dataset caiso_fuel_mix",
        partition_by=["year_month"],
        mode="append",
        storage_options=aws_config
    )
    print("save_on_s3 success!")

    return "Deu bom!"
