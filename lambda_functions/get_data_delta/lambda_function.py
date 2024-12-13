# ================================================================================
# BIBLIOTECA
# ================================================================================

import os
from datetime import datetime, timedelta

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


def handler(event, context):
    """Manipulador de eventos para buscar e processar dados do cliente GridStatus.

    Essa função é acionada por um evento e utiliza a hora atual para definir um intervalo de tempo
    para buscar dados de um dataset específico. Os dados são recuperados do GridStatusClient e,
    em seguida, processados para inclusão em um armazenamento S3.

    Args:
        event: O evento que acionou o manipulador.
        context: O contexto de execução do manipulador.

    Returns:
        str: Mensagem indicando o sucesso da operação.
    """

    # Obtém a data e hora atuais em UTC
    now = datetime.now(tz=ZoneInfo("UTC"))

    # Define o intervalo de tempo para a busca dos dados
    if now.minute // 30 > 0:
        time_str = now.strftime(format="%Y-%m-%dT%H")
        start = f"{time_str}:00"  # Início do intervalo
        end = f"{time_str}:30"  # Fim do intervalo
    else:
        time_str = now.strftime(format="%Y-%m-%dT%H")
        end = f"{time_str}:00"  # Fim do intervalo
        now = now - timedelta(hours=1)  # Retrocede uma hora
        time_str = now.strftime(format="%Y-%m-%dT%H")
        start = f"{time_str}:30"  # Início do intervalo

    # Fetch the data
    print("Fetching dataset from GridStatusClient...")
    print(f"> Start date: {start}\n> End date: {end}")

    # Cria uma instância do cliente GridStatus
    grid_client = GridStatusClient(api_key=GRIDSTATUS_API_KEY)

    # Busca o dataset especificado
    data = grid_client.get_dataset(
        dataset="caiso_fuel_mix",
        start=start,
        end=end,
        tz="UTC",
        limit=QUERY_LIMIT,
    )
    print("Dataset fetched!")

    # Parseia as datas para o formato datetime
    data["interval_start_utc"] = pd.to_datetime(data["interval_start_utc"])
    data["interval_end_utc"] = pd.to_datetime(data["interval_end_utc"])

    # Cria uma coluna para particionar os dados por ano e mês
    data["year_month"] = data["interval_start_utc"].dt.strftime("%Y-%m")

    # Escreve os dados no Delta Lake no S3
    print("save_on_s3 ...")
    aws_config = {"AWS_REGION": "us-east-1", "AWS_S3_ALLOW_UNSAFE_RENAME": "true"}
    write_deltalake(
        "s3://alecrimtechchallengetresbronze/energy_grid_api/",
        data,
        description="Tabela extraída da API pública através do dataset caiso_fuel_mix",
        partition_by=["year_month"],
        mode="append",
        storage_options=aws_config,
    )
    print("save_on_s3 success!")

    return "Deu bom!"
