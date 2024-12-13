import os
from datetime import UTC, datetime, timedelta
from functools import reduce
from io import BytesIO

import boto3
import pandas as pd
from botocore.exceptions import ClientError


def s3_file_exists(bucket: str, file_key: str) -> bool:
    """Verifica se um arquivo existe em um bucket do S3.

    Args:
        bucket (str): O nome do bucket S3.
        file_key (str): A chave do arquivo no bucket.

    Returns:
        bool: Retorna True se o arquivo existir, caso contrário, False.
    """
    s3_client = boto3.client("s3")
    try:
        s3_client.head_object(Bucket=bucket, Key=file_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise Exception(e)


def save_on_s3(bucket: str, s3_file_path: str, data_buffer: BytesIO) -> None:
    """Faz o upload de um arquivo de um buffer BytesIO para o S3.

    Args:
        bucket (str): O nome do bucket S3.
        s3_file_path (str): O caminho onde o arquivo será salvo no S3.
        data_buffer (BytesIO): O buffer em memória contendo os dados a serem salvos.

    Returns:
        None: Esta função não retorna nenhum valor.
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket)
    # Redefine a posição do buffer para o início
    data_buffer.seek(0)
    # Faz o upload dos dados para o S3
    bucket.put_object(Key=s3_file_path, Body=data_buffer)
    return None


def load_parquet_from_s3(object_key: str):
    """Carrega um arquivo Parquet a partir de um bucket S3.

    Args:
        object_key (str): A chave do arquivo Parquet no bucket S3.

    Returns:
        DataFrame: Retorna um DataFrame do pandas com os dados do arquivo Parquet.
    """
    s3 = boto3.resource("s3")
    s3_object = s3.Object(os.getenv("BUCKET_BRONZE"), object_key)
    # Usa o gerenciador de contexto 'with' para gerenciar o buffer
    with BytesIO() as buffer:
        s3_object.download_fileobj(buffer)
        buffer.seek(0)  # Redefine a posição do buffer para o início
        parquet_df = pd.read_parquet(buffer)
    return parquet_df


def handler(event, context):
    """Agrupa os dados das últimas 24h com a predição da próxima meia-hora.

    Args:
        event (dict): Infos do evento que ativou esta função Lambda.
        context (object): O contexto de execução da função Lambda.

    Returns:
        str: Retorna uma mensagem indicando o resultado do processamento.
    """

    s3_resource = boto3.resource("s3")

    # Horário de referência
    agora = datetime.now(tz=UTC)

    # Specify the bucket name
    bronze_layer = os.getenv("BUCKET_BRONZE")  # "'alecrimtechchallengetresbronze'
    gold_layer = os.getenv("BUCKET_GOLD")  # "'alecrimtechchallengetresgold'

    # Filter files within the 'energy_grid' folder and modified in the last 24 hours
    print("Obtendo o path dos dados . . .")
    filtered_files = [
        obj.key
        for obj in s3_resource.Bucket(bronze_layer).objects.all()
        if (obj.last_modified > (agora - timedelta(days=1)))
        and (obj.key.startswith("energy_grid/"))
        and (obj.key.endswith(".parquet"))
    ]
    print("Feito!")

    # Obtendo a última predição
    print("Obtendo a última predição . . .")
    f_reduce = lambda old, new: (
        new if new.last_modified > old.last_modified else old
    )
    last_prediction = reduce(f_reduce, s3_resource.Bucket(bronze_layer).objects.all()).key
    print("Feito!")

    # Carregando os dados
    print("Carregando os dados . . .")
    dfs = [
        load_parquet_from_s3(object_key)
        for object_key in filtered_files + [last_prediction]
    ]

    df = pd.concat(dfs, axis=0, ignore_index=True)
    cols = ["interval_start_utc", "interval_end_utc", "wind"]
    df = df.loc[:, cols].sort_values(by="interval_start_utc", axis=0)
    print("Dados carregados!")

    # Chave do arquivo com os dados de visualização
    s3_file_path = "data_vis/data.parquet"
    print(f"{s3_file_path = }")

    # Escreve o DataFrame em um buffer em memória
    data_buffer = BytesIO()
    print("data.to_parquet ...")
    df.to_parquet(data_buffer, index=False)  # Converte para Parquet
    print("data.to_parquet success!")

    # Faz o upload do DataFrame em formato Parquet para o S3
    print("save_on_s3 ...")
    save_on_s3(
        bucket=gold_layer,
        s3_file_path=s3_file_path,
        data_buffer=data_buffer,
    )  # Faz o upload do arquivo
    print("save_on_s3 success!")

    return "Deu bom!"
