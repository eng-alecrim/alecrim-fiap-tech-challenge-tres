import pickle
import joblib
from sklearn.metrics import root_mean_squared_error

from src.utils import get_path_projeto

dir_projeto = get_path_projeto()
dir_staged = dir_projeto / "data/staged"
dir_models = dir_projeto / "ml_models"

path_train_test_data = dir_staged / "train_test_data.pkl"

with open(path_train_test_data, "rb") as pkl_f:
    train_test_data = pickle.load(file=pkl_f)

X_test = train_test_data["X"]["test"]
y_test = train_test_data["y"]["test"]

scaler = joblib.load(dir_models / "min_max_scaler.joblib")
model = joblib.load(dir_models / "min_max_scaler.joblib")

y_pred = model.predict(X_test).reshape(-1, 1)

rmse = root_mean_squared_error(
    scaler.inverse_transform(y_pred),
    scaler.inverse_transform(y_test)
)

print(f"{rmse = }")