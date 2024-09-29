import logging
from pyspark.sql import DataFrame
from schemas.schemas import schema_df_model, schema_df_model_train, schema_model_results
from fp_functions.model_fit import (
    add_model_features,
    clean_feature_and_generate_test_data,
    fit_and_results
)
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ModelFitAndResults:
    def __init__(self, config):
        """
        Inicializa la clase ModelFitAndResults con la configuración cargada.
        """
        self.model_config = config['model_definitions'][0]
        logger.info("Model configuration loaded from config.json")

    def model_fit_and_results(self, df: DataFrame) -> DataFrame:
        """
        Ajusta el modelo y guarda los resultados y betas.

        Parameters:
        df (DataFrame): DataFrame de entrada.

        Returns:
        DataFrame: DataFrame procesado con los resultados.
        """
        model_config = self.model_config
        num_lags = model_config['num_lags']
        lag_cols = model_config['lag_cols']
        log_cols = model_config['log_cols']
        alpha = model_config['alpha']

        # Agregar características del modelo y columnas de retardos
        df_lags = df.groupBy("sk_material", "sk_tienda").applyInPandas(
            lambda df: add_model_features(
                df,
                num_lags=num_lags, 
                log_cols=log_cols, 
                lag_cols=lag_cols
            ),
            schema=schema_df_model
        )
        logger.info("Added model features and lags columns")
        print("Added model features and lags columns")

        # Eliminar columnas y generar datos de prueba
        df_train = df_lags.groupBy(["sk_material", "sk_tienda"]).applyInPandas(
            clean_feature_and_generate_test_data,
            schema=schema_df_model_train
        )
        logger.info("Dropped columns and generated test data")
        print("Dropped columns and generated test data")

        # Ajustar modelo y guardar resultados y betas
        fecha_ejecucion = datetime.now()
        model_id = pd.to_datetime(fecha_ejecucion).strftime("%Y%m%d%H")

        result_df = df_train.groupBy("sk_material", "sk_tienda").applyInPandas(
            lambda df: fit_and_results(
                df, 
                alpha=alpha, 
                model_id=model_id, 
                fecha_ejecucion=fecha_ejecucion
            ),
            schema=schema_model_results
        )
        logger.info(f"Model fit and results saved on results_df for model_id: {model_id}")
        print(f"Model fit and results saved on results_df for model_id: {model_id}")
        return result_df
