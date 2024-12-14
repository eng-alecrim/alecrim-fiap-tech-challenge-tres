# Bibliotecas
import pickle

import joblib
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler

from src.utils import get_path_projeto

# Diretórios
dir_projeto = get_path_projeto()

dir_staged = dir_projeto / "data/staged"
dir_staged.mkdir(parents=True, exist_ok=True)

dir_models = dir_projeto / "ml_models"
dir_models.mkdir(exist_ok=True)

# 1. Carregando os dados
path_csv = dir_staged / "dados_empilhados.csv"
config_csv = {
    "sep": "\t",
    "encoding": "utf-8"
}

dataset = pd.read_csv(path_csv, **config_csv)

# 2. Selecionando apenas dados sobre a geração de energia eólica
wind_power_generation = dataset.loc[:, ["interval_start_local", "wind"]]
wind_power_generation.rename(
    columns={"interval_start_local": "date", "wind": "power_generation"},
    inplace=True
)

# 3. Obtendo os valores da coluna de geração de energia
wind_power_generation_values = wind_power_generation["power_generation"].values


# 4. Normalizando os dados
scaler = MinMaxScaler()
wind_power_generation_scaled_values = scaler.fit_transform(wind_power_generation_values.reshape(-1, 1)).ravel()

# 5. Criando os dados de "features" e "target"
window_len = 6  # Número de pontos que existem por meia hora
X = []
y = []

for i in range(len(wind_power_generation_scaled_values) - window_len):
    X.append(wind_power_generation_scaled_values[i:i + window_len])
    y.append(wind_power_generation_scaled_values[i + window_len])

X = np.array(X)
y = np.array(y)

# 6. Dividindo os dados de treino e de teste
X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=0.8, random_state=42)

# 7. Salvando para uso futuro

# Scaler
joblib.dump(scaler, dir_models / "min_max_scaler.joblib")

# Dados de treino e teste
train_test_data = {
    "X": {
        "raw": X,
        "train": X_train,
        "test": X_test
    },
    "y": {
        "raw": y,
        "train": y_train,
        "test": y_test
    }
}

with open(dir_staged / "train_test_data.pkl", "wb") as pkl_f:
    pickle.dump(obj=train_test_data, file=pkl_f)