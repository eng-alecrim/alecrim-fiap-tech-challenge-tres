from gridstatusio import GridStatusClient
from deltalake.writer import write_deltalake

grid_client = GridStatusClient() #api_key=**************
QUERY_LIMIT = 600_000

data = grid_client.get_dataset(
    dataset="caiso_fuel_mix",
    start='2019-01-01',
    end='2024-10-31',
    tz="America/Sao_Paulo",
    limit=QUERY_LIMIT,
)

data['year'] = data['interval_start_local'].dt.year
data = data.sort_values(by="interval_start_local").reset_index(drop=True)

write_deltalake('./lake/delta_table', data,
                description= 'Tabela extraída da API pública através do dataset caiso_fuel_mix',
                partition_by= ['year'])