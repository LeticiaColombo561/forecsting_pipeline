import numpy as np
import pandas as pd
from typing import List
from sklearn.linear_model import Ridge
from sklearn.metrics import r2_score

def add_lag_and_log_features(df: pd.DataFrame, 
                             lag_cols: List[str], 
                             num_lags: int, 
                             log_cols: List[str]) -> pd.DataFrame:
    df = df.copy()

    for col in lag_cols:
        for lag in range(1, num_lags + 1):
            df[f"{col}_lag{lag}"] = df[col].shift(lag)

    for col in log_cols:
        df[f"{col}_log"] = np.log(df[col].clip(lower=0.001))
        for lag in range(1, num_lags + 1):
            lag_col = f"{col}_lag{lag}"
            if lag_col in df.columns:
                df[f"{lag_col}_log"] = np.log(df[lag_col].clip(lower=0.001))
    return df


def add_model_features(df: pd.DataFrame, 
                       num_lags: int = 2, 
                       log_cols: List[str] = ['precio_ratio', 'cnt_venta'], 
                       lag_cols: List[str] = ['precio_ratio']) -> pd.DataFrame:
    df_lags = df.copy().sort_values("fecha").reset_index(drop=True)
    df_lags["fecha"] = pd.to_datetime(df["fecha"])

    lag_cols = ['precio_ratio']
    num_lags = 2
    log_cols = ['precio_ratio', 'cnt_venta']

    df_lags = add_lag_and_log_features(df_lags, lag_cols, num_lags, log_cols)

    df_lags = df_lags.iloc[num_lags:].reset_index(drop=True)

    return df_lags

def clean_feature_and_generate_test_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add colum indicating if the row is in the test set
    """
    max_date = df.fecha.max()
    train_size = max_date - pd.DateOffset(days=30)

    df['test'] = np.where(df.fecha > train_size, 1, 0)

    cols_to_drop = ["fecha", "cnt_venta", "precio_ratio", 
                    "precio_ratio_lag1", "precio_ratio_lag2"]
    df = df.drop(columns = cols_to_drop)

    # Reorder columns
    cols= ['cnt_venta_log', 'precio_ratio_log', 'precio_ratio_lag1_log', 'precio_ratio_lag2_log']
    cols_reorder = cols + [col for col in df.columns if col not in cols]
    df = df[cols_reorder]

    return df


def calculate_metrics(y_true, y_pred) -> dict:

    r2 = r2_score(y_true, y_pred)
    metrics = {"r2": r2}
    return metrics


def fit_and_results(df: pd.DataFrame, 
                    alpha: float, 
                    model_id: str, 
                    fecha_ejecucion: pd.Timestamp) -> pd.DataFrame:

    """
    Ajustar el modelo Ridge y predecir para cada combinación de sk_material y sk_tienda.
    Devuelve un DataFrame con las métricas.
    """
    target = 'cnt_venta_log'
    cols_drop = ['sk_material', 'sk_tienda', 'test', 'cnt_venta_log']
    
    # Split train and test
    X_train = df.drop(cols_drop, axis=1)
    y_train = df[target]
    
    X_test = df[df['test'] == 1].drop(cols_drop, axis=1)
    y_test = df[df['test'] == 1][target]

    # Ridge model with alpha
    # alpha = 3
    model = Ridge(alpha=alpha)
    model.fit(X_train, y_train)
    
    # Predict and metric
    y_pred = model.predict(X_test)
    r2 = r2_score(y_test, y_pred)

    # R2 ajustado
    n = len(y_test)
    p = X_test.shape[1]
    r2_adj = 1 - (1 - r2) * (n - 1) / (n - p - 1)

    # Betas dataframe
    betas = {"const": model.intercept_}
    betas.update(dict(zip(X_train.columns, model.coef_)))
    
    # Beta of precio_ratio_log
    beta_elasticity = betas.get('precio_ratio_log', 0)

    # Obtener sk_material y sk_tienda
    sk_material = str(df['sk_material'].iloc[0])
    sk_tienda = str(df['sk_tienda'].iloc[0])
    
    # Crear un DataFrame con las métricas
    result = pd.DataFrame({
        'model_id': [model_id],
        'sk_material': [sk_material],
        'sk_tienda': [sk_tienda],
        'alpha': [alpha],
        'r2': [r2_adj], 
        'betas': [str(betas)],
        'beta_elasticity': [beta_elasticity],
        'fecha_ejecucion': [fecha_ejecucion],
    })

    return result