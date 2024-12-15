import streamlit as st
import matplotlib.pyplot as plt
from utils import *

st.title('Previsão de Produção de Energia')
st.sidebar.header('Configurações')

frequencias = {
    "hora": {"freq": 'h', "start": "2024-09-01", "end": "2024-11-01", "periods": 24},
    "dia": {"freq": 'd', "start": "2019-01-01", "end": "2024-11-01", "periods": 365},
    "mes": {"freq": 'M', "start": "2019-01", "end": "2024-11", "periods": 2}
}

# Lista de tipos de energia
energias = ['solar', 'wind', 'geothermal', 'biomass', 'biogas', 'small_hydro', 'coal', 'nuclear',
          'natural_gas', 'large_hydro', 'batteries', 'imports']


modelo_selecionado = st.sidebar.selectbox('Tipo de Modelo', ('Por periodo', 'A cada 5 min',))

if modelo_selecionado == 'A cada 5 min':
    previsao_horizonte = st.sidebar.slider('Previsão Horizonte (em minutos)', min_value=5, max_value=30, step=5, value=30)
    df_day, df_prediction = forecast_wind(previsao_horizonte)

    fig = plotar_grafico(df_day, df_prediction, previsao_horizonte)

    st.plotly_chart(fig, use_container_width=True)


if modelo_selecionado == 'Por periodo':
    tipos_energia = st.sidebar.multiselect('Selecione os tipos de energia para visualizar:', 
                                       energias, 
                                       default=['wind', 'biomass', 'biogas', 'small_hydro', 'natural_gas'])
    periodo_selecionado = st.sidebar.selectbox(
            "Selecione o período para a previsão:",
            ["hora", "dia", "mes"]
        )

    # Configuração de períodos com base no período selecionado
    if periodo_selecionado == 'hora':
        periods = st.sidebar.slider('Quantas horas deseja prever?', min_value=1, max_value=24, step=1, value=24)
    elif periodo_selecionado == 'dia':
        periods = st.sidebar.slider('Quantos dias deseja prever?', min_value=1, max_value=365, step=1, value=365)
    elif periodo_selecionado == 'mes':
        periods = st.sidebar.slider('Quantos meses deseja prever?', min_value=1, max_value=3, step=1, value=2)

    # Loop para prever e exibir gráficos para cada energia selecionada
    for energy_type in tipos_energia:
        st.subheader(f"Previsão para {energy_type.capitalize()} ({periodo_selecionado.capitalize()})")
        base_path_model = f"ml_models/{energy_type}/"
        model_path = f"prophet_{periodo_selecionado}.joblib"

        model = carregar_modelo(base_path_model + model_path)  # Carregar o modelo correspondente
        forecast = prever(model, periods)  # Gerar a previsão

        plt.figure()
        fig = model.plot(forecast)  # Gerar o gráfico
        plt.title(f"Previsão para {energy_type.capitalize()} ({periodo_selecionado})")

        st.pyplot(fig)  # Exibir o gráfico