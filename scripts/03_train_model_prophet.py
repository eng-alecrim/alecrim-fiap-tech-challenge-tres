import joblib
from deltalake import DeltaTable
from prophet import Prophet
import pandas as pd

# Carregar e processar o dataframe
print("Carregando dados da DeltaTable...")
df = DeltaTable('lake/delta_table').to_pandas()
df = df.sort_values(by='interval_start_local')

def prepara_base_para_treino(df, freq, start, end):
    """Prepara a base para treino agregando os dados pela frequência especificada."""
    df_agg = df.groupby(df['interval_start_local'].dt.to_period(freq))['wind'].median().reset_index()
    df_agg.rename(columns={"interval_start_local": "ds", "wind": "y"}, inplace=True)
    df_agg['ds'] = pd.to_datetime(df_agg['ds'].astype(str))
    df_filtered = df_agg[(df_agg["ds"] >= start) & (df_agg["ds"] <= end)]
    df_filtered.reset_index(drop=True, inplace=True)
    return df_filtered

def treina_e_salva_modelo(df_train, output_file):
    """Treina o modelo Prophet e salva em um arquivo."""
    print(f"Treinando modelo para {output_file}...")
    model = Prophet()
    model.fit(df_train)
    joblib.dump(model, output_file)
    return model

# def carregar_modelo(model_path):
#     """Carrega o modelo Prophet e realiza a previsão."""
#     print(f"Carregando modelo de {model_path}...")
#     model = joblib.load(model_path)
#     return model

# def prever(model, periods):
#     future = model.make_future_dataframe(periods=periods)
#     forecast = model.predict(future)
#     return forecast

# Configurações de agregação e períodos de treino
frequencias = {
    "hora": {"freq": 'h', "start": "2024-09-01", "end": "2024-11-01", "periods": 24},
    "dia": {"freq": 'd', "start": "2019-01-01", "end": "2024-11-01", "periods": 365},
    "mes": {"freq": 'M', "start": "2019-01", "end": "2024-11", "periods": 2}
}

# Processar dados, treinar e salvar modelos
modelos = {}
for periodo, config in frequencias.items():
    df_train = prepara_base_para_treino(df, config['freq'], config['start'], config['end'])
    model_path = f"ml_models/prophet_por_{periodo}.joblib"
    modelos[periodo] = treina_e_salva_modelo(df_train, model_path)
    del df_train

del df