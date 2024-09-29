import pytest
from pyspark.sql import SparkSession
from schemas.names import (DfVentasPreprocessedNames)
from schemas.schemas import schema_df_dates, schema_model_results
from forecasting_promocional.model_fit_and_predict import ModelFitAndResults
from pyspark.sql import functions as F
import pandas as pd
import numpy as np


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("ModelFitAndResultsTest") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def sample_data(spark):
    """Generate sample data for testing."""
    np.random.seed(0) 
    n_rows = 1500
    valid_dates = pd.date_range(start='2023-01-14', end='2024-12-21', periods=720)

    data = pd.DataFrame(
            {
            DfVentasPreprocessedNames.SK_MATERIAL.value: np.random.choice([1001, 1001], size=n_rows),
            DfVentasPreprocessedNames.SK_TIENDA.value: np.random.choice([20, 30], size=n_rows),
            DfVentasPreprocessedNames.FECHA.value: np.random.choice(valid_dates, size=n_rows),
            DfVentasPreprocessedNames.CNT_VENTA.value: np.round(np.random.uniform(1, 200, size=n_rows), 3),
            DfVentasPreprocessedNames.PRECIO_RATIO.value: np.round(np.random.uniform(0, 1, size=n_rows), 2),

            DfVentasPreprocessedNames.AÑO_NUEVO.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.JUEVES_SANTO.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.VIERNES_SANTO.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.DOMINGO_DE_RESURRECCION.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.DIA_DEL_TRABAJO.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.SAN_PEDRO_Y_SAN_PABLO.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.DIA_DE_LA_INDEPENDENCIA.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.DIA_DE_LA_GRAN_PARADA_MILITAR.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.BATALLA_DE_JUNIN.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.SANTA_ROSA_DE_LIMA.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.COMBATE_DE_ANGAMOS.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.TODOS_LOS_SANTOS.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.INMACULADA_CONCEPCION.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.BATALLA_DE_AYACUCHO.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.NAVIDAD_DEL_SEÑOR.value: np.random.choice([True, False], size=n_rows),

            DfVentasPreprocessedNames.WEEKEND_TRUE.value: np.random.choice([True, False], size=n_rows),

            DfVentasPreprocessedNames.DAY_OF_WEEK_2.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.DAY_OF_WEEK_3.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.DAY_OF_WEEK_4.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.DAY_OF_WEEK_5.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.DAY_OF_WEEK_6.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.DAY_OF_WEEK_7.value: np.random.choice([True, False], size=n_rows),

            DfVentasPreprocessedNames.WEEK_OF_MONTH_2.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.WEEK_OF_MONTH_3.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.WEEK_OF_MONTH_4.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.WEEK_OF_MONTH_5.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.WEEK_OF_MONTH_6.value: np.random.choice([True, False], size=n_rows),

            DfVentasPreprocessedNames.MONTH_2.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.MONTH_3.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.MONTH_4.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.MONTH_5.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.MONTH_6.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.MONTH_7.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.MONTH_8.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.MONTH_9.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.MONTH_10.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.MONTH_11.value: np.random.choice([True, False], size=n_rows),
            DfVentasPreprocessedNames.MONTH_12.value: np.random.choice([True, False], size=n_rows),
            }
            )


    df = spark.createDataFrame(data, schema=schema_df_dates)
    return df

def test_model_fit_and_results(spark, sample_data):
    """Test the model fitting and results."""
    # Create a configuration for the model
    config = {
        'model_definitions': [{
            'num_lags': 2,
            'lag_cols': ['precio_ratio_lag1', 'precio_ratio_lag2'],
            'log_cols': ['cnt_venta_log', 'precio_ratio_log'],
            'alpha': 1.0
        }]
    }

    modelfit = ModelFitAndResults(config)
    result_df = modelfit.model_fit_and_results(sample_data)

    # Verify results DataFrame schema
    assert result_df.schema == schema_model_results

    # Example checks
    assert result_df.count() > 0  
    assert result_df.select("model_id").distinct().count() == 1
    assert result_df.select("sk_material").distinct().count() == 1
    assert result_df.select("sk_tienda").distinct().count() == 2
    assert result_df.select("beta_elasticity").rdd.flatMap(lambda x: x).collect() == [-0.04044162109494209, 
                                                                                      0.04978546127676964]


if __name__ == "__main__":
    pytest.main()
