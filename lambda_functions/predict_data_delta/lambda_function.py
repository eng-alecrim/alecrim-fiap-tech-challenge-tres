# ================================================================================
# BIBLIOTECA
# ================================================================================

import os
from io import BytesIO

import boto3
import joblib
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError
from deltalake.writer import write_deltalake

# ================================================================================
# CONSTANTES
# ================================================================================

# Nome do bucket
BUCKET_DATA = os.getenv("BUCKET_DATA")  # "alecrimtechchallengetresbronze"
BUCKET_MODELS = os.getenv("BUCKET_MODELS")  # "alecrimtechchallengetressilver"

# ================================================================================
# FUNÇÕES
# ================================================================================


def get_s3_latest_parquet() -> str:
    """Obtém a chave do arquivo Parquet mais recente de um bucket S3.

    Esta função se conecta a um bucket S3 e lista todos os objetos com um prefixo específico.
    Ela filtra os arquivos que terminam com a extensão '.parquet' e determina qual deles
    foi modificado mais recentemente. O resultado é a chave do arquivo mais recente.

    Returns:
        str: A chave do arquivo Parquet mais recentemente modificado no bucket S3.
    """

    # Cria um cliente S3 usando a biblioteca boto3
    s3 = boto3.client("s3")

    # Nome do bucket e prefixo para filtrar os objetos
    bucket_name = "alecrimtechchallengetresbronze"
    prefix = "energy_grid_api/"

    # Lista os objetos no bucket filtrando para arquivos .parquet
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Filtra os arquivos Parquet e armazena suas chaves e datas de última modificação
    parquet_files = [
        {"Key": obj["Key"], "LastModified": obj["LastModified"]}
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    # Encontra o arquivo Parquet mais recentemente modificado
    last_modified_file = max(parquet_files, key=lambda x: x["LastModified"])

    # Exibe a chave do arquivo e a data da última modificação
    print(f"Key: {last_modified_file['Key']}")
    print(f"LastModified: {last_modified_file['LastModified']}")

    # Retorna a chave do arquivo mais recente
    return last_modified_file["Key"]


def s3_file_exists(bucket: str, file_key: str) -> bool:
    """Verifica se um arquivo existe em um bucket S3.

    Esta função utiliza o cliente S3 da biblioteca boto3 para verificar a existência de um arquivo
    específico em um bucket S3. Ela tenta acessar os metadados do objeto usando a operação head_object.
    Se o arquivo for encontrado, retorna True. Se o arquivo não existir (404), retorna False.
    Para outros erros, uma exceção é levantada.

    Args:
        bucket (str): O nome do bucket S3 onde o arquivo está localizado.
        file_key (str): A chave do arquivo a ser verificado.

    Returns:
        bool: Retorna True se o arquivo existir, False se não existir.
    """

    # Cria um cliente S3 usando a biblioteca boto3
    s3_client = boto3.client("s3")

    try:
        # Tenta obter os metadados do objeto no S3
        s3_client.head_object(Bucket=bucket, Key=file_key)
        return True  # O arquivo existe
    except ClientError as e:
        # Se o erro for 404, o arquivo não existe
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            # Levanta uma exceção para outros erros
            raise Exception(e)


def save_on_s3(bucket: str, s3_file_path: str, data_buffer: BytesIO) -> None:
    """Faz upload de um arquivo de um buffer BytesIO para o S3.

    Esta função utiliza a biblioteca boto3 para enviar um arquivo que está em memória
    (em um objeto BytesIO) para um bucket S3 específico. O caminho do arquivo no S3
    é fornecido como um argumento.

    Args:
        bucket (str): O nome do bucket S3 onde o arquivo será salvo.
        s3_file_path (str): O caminho (key) onde o arquivo será armazenado no bucket S3.
        data_buffer (BytesIO): O buffer em memória contendo os dados a serem enviados.

    Returns:
        None: Esta função não retorna nada.
    """

    # Cria um recurso S3 usando a biblioteca boto3
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket)

    # Reseta a posição do buffer para o início
    data_buffer.seek(0)

    # Faz o upload dos dados para o S3
    bucket.put_object(Key=s3_file_path, Body=data_buffer)

    return None


def load_joblib_from_s3(object_key: str):
    """Carrega um objeto joblib armazenado em um bucket S3.

    Esta função utiliza a biblioteca boto3 para baixar um objeto armazenado em S3
    e carrega-o como um objeto joblib. O objeto é baixado para um buffer em memória
    (BytesIO), que é então usado para carregar o objeto joblib.

    Args:
        object_key (str): A chave do objeto no bucket S3 que contém o arquivo joblib.

    Returns:
        object: O objeto joblib carregado a partir do buffer.
    """

    # Cria um recurso S3 usando a biblioteca boto3
    s3 = boto3.resource("s3")
    s3_object = s3.Object(BUCKET_MODELS, object_key)

    # Usa o gerenciador de contexto with para gerenciar o buffer
    with BytesIO() as buffer:
        # Faz o download do objeto S3 para o buffer
        s3_object.download_fileobj(buffer)
        buffer.seek(0)  # Reseta a posição do buffer para o início

        # Carrega o objeto joblib a partir do buffer
        joblib_object = joblib.load(buffer)

    return joblib_object


def load_parquet_from_s3(object_key: str):
    """Carrega um arquivo Parquet armazenado em um bucket S3 como um DataFrame.

    Esta função utiliza a biblioteca boto3 para baixar um arquivo Parquet armazenado em S3
    e carregá-lo em um DataFrame do pandas. O arquivo é baixado para um buffer em memória
    (BytesIO), que é então usado para ler o arquivo Parquet.

    Args:
        object_key (str): A chave do objeto no bucket S3 que contém o arquivo Parquet.

    Returns:
        pd.DataFrame: Um DataFrame do pandas contendo os dados do arquivo Parquet.
    """

    # Cria um recurso S3 usando a biblioteca boto3
    s3 = boto3.resource("s3")
    s3_object = s3.Object(BUCKET_DATA, object_key)

    # Usa o gerenciador de contexto with para gerenciar o buffer
    with BytesIO() as buffer:
        # Faz o download do objeto S3 para o buffer
        s3_object.download_fileobj(buffer)
        buffer.seek(0)  # Reseta a posição do buffer para o início

        # Lê o arquivo Parquet a partir do buffer e o carrega em um DataFrame
        parquet_df = pd.read_parquet(buffer)

    return parquet_df


def predict_meia_hora(x, scaler, model):
    """Realiza previsões de valores futuros com um modelo de aprendizado de máquina.

    Esta função utiliza um modelo previamente treinado para prever valores futuros
    com base em dados de entrada fornecidos. Os dados de entrada são escalonados
    usando um scaler, e as previsões são feitas em uma janela deslizante.

    Args:
        x (np.ndarray): Os dados de entrada que serão usados para a previsão.
        scaler: O scaler utilizado para transformar os dados de entrada.
        model: O modelo de aprendizado de máquina que fará as previsões.

    Returns:
        np.ndarray: Um array contendo as previsões feitas pelo modelo.
    """

    # Escala os dados de entrada e ajusta a forma
    x_scaled = scaler.transform(x).reshape(1, 6)

    # Inicializa a previsão com o último valor escalonado
    forsee = np.array(x_scaled[0, -1]).ravel()

    # Define parâmetros para o loop de previsão
    num_points = 6
    num_forsee = 6
    tamanho_janela = 6

    # Realiza previsões em uma janela deslizante
    for i in range(num_forsee):
        if i == 0:
            # Faz a primeira previsão
            a = model.predict(x_scaled).reshape(-1, 1)
            b = x_scaled[:num_points][-tamanho_janela:].reshape(1, tamanho_janela)[
                :, -(tamanho_janela - 1) :
            ]
            c = np.concatenate((b, a), axis=1)
            forsee = np.concatenate((forsee, a.ravel()))
            continue

        # Faz previsões subsequentes
        a = model.predict(c).reshape(-1, 1)
        b = c[[-1], -(tamanho_janela - 1) :]
        c = np.concatenate((b, a), axis=1)
        forsee = np.concatenate((forsee, a.ravel()))

    # Inverte a transformação dos dados escalonados para obter os valores reais
    forsee = np.vectorize(
        lambda x: scaler.inverse_transform(np.array(x).reshape(-1, 1))
    )(forsee)

    return forsee[1:]  # Retorna as previsões, excluindo o primeiro valor


def handler(event, context):
    """Manipulador principal para processar eventos e gerar previsões de energia.

    Esta função é o ponto de entrada para o processamento de eventos. Ela obtém a chave do objeto
    mais recente em S3, carrega um modelo de regressão e um scaler, e faz previsões de dados de
    energia. Os resultados são então organizados em um DataFrame e enviados de volta para o S3.

    Args:
        event: O evento que aciona a função (ex: um evento de API Gateway).
        context: O contexto de execução da função, que fornece informações sobre a invocação.

    Returns:
        str: Mensagem de sucesso ou erro.
    """

    print("Obtendo a chave do objeto ...")
    # Obtém o caminho do arquivo Parquet mais recente no S3
    s3_file_path = get_s3_latest_parquet()
    print(f"{s3_file_path = }")

    # Verifica se o arquivo existe no bucket S3
    if not s3_file_exists(bucket=BUCKET_DATA, file_key=s3_file_path):
        print(f"Arquivo não encontrado! '{s3_file_path}'")
        return f"Arquivo não encontrado! '{s3_file_path}'"

    # Carrega o modelo e o scaler do S3
    model = load_joblib_from_s3("models/regression_model.joblib")
    print("Modelo carregado!")
    scaler = load_joblib_from_s3("models/min_max_scaler.joblib")
    print("Scaler carregado!")

    # Carrega os dados de energia do arquivo Parquet
    energy_grid_data = load_parquet_from_s3(s3_file_path)
    print("Dados carregados!")

    # Realiza a previsão com base nos dados de vento
    wind_data = energy_grid_data["wind"].values
    prox_meia_hora = predict_meia_hora(wind_data.reshape(-1, 1), scaler, model)
    print("Previsão feita!")

    # Cria um DataFrame com as colunas necessárias
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

    # Adiciona as previsões subsequentes ao DataFrame
    for valor in prox_meia_hora[1:]:
        new_row = {
            "interval_start_utc": rows[-1]["interval_start_utc"]
            + pd.Timedelta(minutes=5),
            "interval_end_utc": rows[-1]["interval_end_utc"] + pd.Timedelta(minutes=5),
            "wind": valor,
        }
        rows.append(new_row)

    df_predicted = pd.DataFrame(rows)
    df_predicted["year_month"] = df_predicted["interval_start_utc"].dt.strftime("%Y-%m")
    print("Dataframe criado!")

    # Faz o upload dos dados preditos para o S3
    print("save_on_s3 ...")
    aws_config = {"AWS_REGION": "us-east-1", "AWS_S3_ALLOW_UNSAFE_RENAME": "true"}
    write_deltalake(
        "s3://alecrimtechchallengetresbronze/predicted_data/",
        df_predicted,
        description="Dados preditos pelo modelo de regressão.",
        partition_by=["year_month"],
        mode="append",
        storage_options=aws_config,
    )
    print("save_on_s3 success!")

    return "Deu bom!"
