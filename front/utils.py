from deltalake import DeltaTable
import pandas as pd
import plotly.graph_objects as go
import joblib
import numpy as np
from prophet import Prophet

scaler = joblib.load('ml_models/min_max_scaler.joblib')
model_lgbm = joblib.load('ml_models/lgbm.joblib')

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
    #print(f"Treinando modelo para {output_file}...")
    model = Prophet()
    model.fit(df_train)
    joblib.dump(model, output_file)
    return model

def carregar_modelo(model_path):
    """Carrega o modelo Prophet e realiza a previsão."""
    #print(f"Carregando modelo de {model_path}...")
    model = joblib.load(model_path)
    return model

def prever(model, periods):
    """Realiza previsão usando o modelo Prophet."""
    future = model.make_future_dataframe(periods=periods)
    forecast = model.predict(future)
    return forecast

def forecast_wind(previsao_horizonte):
    model_por_5min = carregar_modelo('ml_models/lgbm.joblib')
    
    df = DeltaTable('lake/delta_table').to_pandas()
    df_wind = df[['interval_start_local', 'wind']]
    del df

    df_wind = df_wind.sort_values(by='interval_start_local')

    quantidade_de_retornos = 6
    intervalo_minutos = 5

    last_time = df_wind['interval_start_local'].iloc[-2]
    start_time = last_time - pd.Timedelta(minutes=5 * quantidade_de_retornos)

    df_test = df_wind.loc[(df_wind["interval_start_local"] > start_time) & (df_wind["interval_start_local"] <= last_time)]
    X_test = df_test['wind'].values

    X_test_scaled = scaler.transform(X_test.reshape(-1, 1))

    future_predictions = []
    future_times = []

    current_scaled = X_test_scaled.copy()
    current_time = last_time

    for _ in range(previsao_horizonte // intervalo_minutos):
        y_pred = model_por_5min.predict(current_scaled.reshape(1, -1)).reshape(-1, 1)
        next_wind = scaler.inverse_transform(y_pred)[0, 0]

        future_predictions.append(next_wind)
        current_time += pd.Timedelta(minutes=intervalo_minutos)
        future_times.append(current_time)

        current_scaled = current_scaled[1:, :]  # Remover o valor mais antigo
        current_scaled = np.append(current_scaled, scaler.transform([[next_wind]]), axis=0)  # Adicionar a nova previsão

    df_prediction = pd.DataFrame({'interval_start_local': future_times, 'wind': future_predictions})

    last_day = df_wind["interval_start_local"].dt.floor("D").max()
    df_day = df_wind[df_wind["interval_start_local"].dt.floor("D") == last_day]

    return df_day, df_prediction

def plotar_grafico(df_day, df_prediction, previsao_horizonte):
    """Função para criar e exibir o gráfico interativo."""
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_day['interval_start_local'], 
        y=df_day['wind'],
        mode='lines',
        name='Histórico do Dia',
        line=dict(color='blue')
    ))

    if previsao_horizonte == 5:
        fig.add_trace(go.Scatter(
            x=df_prediction['interval_start_local'], 
            y=df_prediction['wind'],
            mode='markers',
            name='Predição (5 Minutos)',
            marker=dict(color='red', size=10)
        ))
    else:
       
        fig.add_trace(go.Scatter(
            x=df_prediction['interval_start_local'], 
            y=df_prediction['wind'],
            mode='lines',
            name=f'Predição (Próximos {previsao_horizonte} Minutos)',
            line=dict(color='red', dash='dash')
        ))

    fig.update_layout(
        title='Variação do Vento com Predição',
        xaxis_title='Horário',
        yaxis_title='Velocidade do Vento (km/h)',
        xaxis=dict(tickformat='%H:%M', title_standoff=20),
        legend=dict(x=0.01, y=0.99),
        template='plotly_white'
    )

    return fig