# ================================================================================
# BIBLIOTECA
# ================================================================================

import os
from datetime import datetime, timedelta
from io import BytesIO

import boto3
import joblib
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError
from zoneinfo import ZoneInfo

# ================================================================================
# CONSTANTES
# ================================================================================

# Nome do bucket
BUCKET_DATA = os.getenv("BUCKET_DATA")  # "alecrimtechchallengetresbronze"
BUCKET_MODELS = os.getenv("BUCKET_MODELS")  # "alecrimtechchallengetressilver"

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


def load_joblib_from_s3(object_key: str):

    s3 = boto3.resource("s3")
    s3_object = s3.Object(BUCKET_MODELS, object_key)

    # Use the with statement to manage the buffer context
    with BytesIO() as buffer:
        s3_object.download_fileobj(buffer)
        buffer.seek(0)  # Reset the buffer's position to the beginning
        joblib_object = joblib.load(buffer)

    return joblib_object


def load_parquet_from_s3(object_key: str):

    s3 = boto3.resource("s3")
    s3_object = s3.Object(BUCKET_DATA, object_key)

    # Use the with statement to manage the buffer context
    with BytesIO() as buffer:
        s3_object.download_fileobj(buffer)
        buffer.seek(0)  # Reset the buffer's position to the beginning
        parquet_df = pd.read_parquet(buffer)

    return parquet_df


def predict_meia_hora(x, scaler, model):
    x_scaled = scaler.transform(x).reshape(1, 6)

    forsee = np.array(x_scaled[0, -1]).ravel()
    num_points = 6
    num_forsee = 6
    tamanho_janela = 6

    for i in range(num_forsee):
        if i == 0:
            a = model.predict(x_scaled).reshape(-1, 1)
            b = x_scaled[:num_points][-tamanho_janela:].reshape(1, tamanho_janela)[
                :, -(tamanho_janela - 1) :
            ]
            c = np.concatenate((b, a), axis=1)
            forsee = np.concatenate((forsee, a.ravel()))
            continue
        a = model.predict(c).reshape(-1, 1)
        b = c[[-1], -(tamanho_janela - 1) :]
        c = np.concatenate((b, a), axis=1)
        forsee = np.concatenate((forsee, a.ravel()))

    forsee = np.vectorize(
        lambda x: scaler.inverse_transform(np.array(x).reshape(-1, 1))
    )(forsee)

    return forsee[1:]


def handler(event, context):

    print("Obtendo a chave do objeto ...")
    s3_file_path = event["Records"][0]["s3"]["object"]["key"]
    print(f"{s3_file_path = }")

    if not s3_file_exists(bucket=BUCKET_DATA, file_key=s3_file_path):
        print(f"Arquivo não encontrado! '{s3_file_path}'")
        return f"Arquivo não encontrado! '{s3_file_path}'"

    # Fetch the data
    model = load_joblib_from_s3("models/svr.joblib")
    print("Modelo carregado!")
    scaler = load_joblib_from_s3("models/min_max_scaler.joblib")
    print("Scaler carregado!")
    energy_grid_data = load_parquet_from_s3(s3_file_path)
    print("Dados carregados!")

    wind_data = energy_grid_data["wind"].values
    prox_meia_hora = predict_meia_hora(wind_data.reshape(-1, 1), scaler, model)
    print("Previsão feita!")

    cols = ["interval_start_utc", "interval_end_utc", "wind"]
    df = energy_grid_data[cols].copy()

    rows = [
        {
            "interval_start_utc": df["interval_start_utc"].iloc[-1]
            + pd.Timedelta(minutes=5),
            "interval_end_utc": df["interval_end_utc"].iloc[-1]
            + pd.Timedelta(minutes=5),
            "wind": prox_meia_hora[0],
        }
    ]

    for valor in prox_meia_hora[1:]:
        new_row = {
            "interval_start_utc": rows[-1]["interval_start_utc"]
            + pd.Timedelta(minutes=5),
            "interval_end_utc": rows[-1]["interval_end_utc"] + pd.Timedelta(minutes=5),
            "wind": valor,
        }
        rows.append(new_row)
    df_predicted = pd.DataFrame(rows)
    print("Dataframe criado!")

    # Write parquet to an in-memory buffer
    data_buffer = BytesIO()
    print("data.to_parquet ...")
    df_predicted.to_parquet(data_buffer, index=False)
    print("data.to_parquet success!")

    # Upload to S3
    print("save_on_s3 ...")

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

    s3_file_path = f"predicted/{file_name}"

    save_on_s3(
        bucket=BUCKET_DATA,
        s3_file_path=s3_file_path,
        data_buffer=data_buffer,
    )
    print("save_on_s3 success!")

    return "Deu bom!"
