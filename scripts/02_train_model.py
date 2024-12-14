import pickle
import joblib
from sklearn.metrics import root_mean_squared_error
from lightgbm import LGBMRegressor

from src.utils import get_path_projeto

dir_projeto = get_path_projeto()
dir_staged = dir_projeto / "data/staged"
dir_models = dir_projeto / "ml_models"

# 1. Carregando os dados
path_train_test_data = dir_staged / "train_test_data.pkl"

with open(path_train_test_data, "rb") as pkl_f:
    train_test_data = pickle.load(file=pkl_f)

X_train = train_test_data["X"]["train"]
y_train = train_test_data["y"]["train"]

# 2. Scaler
scaler = joblib.load(dir_models / "min_max_scaler.joblib")

model = LGBMRegressor()
model.fit(X_train, y_train)

joblib.dump(model, dir_models / "svr.joblib")