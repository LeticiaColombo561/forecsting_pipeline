# test_data_loaded.py
import pytest
from unittest.mock import MagicMock
from forecasting_promocional.data_loaded import LoadDataVentas
from fp_functions.df_query import query_ventas_marcas_propias

@pytest.fixture
def mock_spark(mocker):
    return mocker.MagicMock()

@pytest.fixture
def mock_query_ventas_marcas_propias(mocker):
    return mocker.patch('forecasting_promocional.data_loaded.query_ventas_marcas_propias')

def test_load_data(mock_spark, mock_query_ventas_marcas_propias):
    # Arrange
    categorias = ["PANADERIA", "ACEITES COMESTIBLES", "POLLOS ROSTIZADO", "PASTELERIA", 
            "HUEVOS", "CARNES ROJAS RES", "CONFITERIA", "ARROZ", "AZUCAR Y EDULCORANTES"]
    tiendas = ["31, 32, 33, 36, 76, 89, 111, 137, 138, 140"]
    query_result = MagicMock()
    query_result.count.return_value = 100

    # Set up mocks
    mock_query_ventas_marcas_propias.return_value = query_ventas_marcas_propias(categorias=categorias, tiendas=tiendas)
    query_ventas = mock_query_ventas_marcas_propias.return_value
    mock_spark.sql.return_value = query_result

    # Act
    loader = LoadDataVentas(spark=mock_spark, categorias=categorias, tiendas=tiendas)
    df_ventas = loader.load_data()

    # Assert
    mock_query_ventas_marcas_propias.assert_called_once_with(categorias=categorias, tiendas=tiendas)
    mock_spark.sql.assert_called_once_with(query_ventas)
    assert df_ventas == query_result

def test_load_data_logs_info(mocker, mock_spark, mock_query_ventas_marcas_propias):
    # Arrange
    categorias = ["PANADERIA", "ACEITES COMESTIBLES", "POLLOS ROSTIZADO", "PASTELERIA", 
            "HUEVOS", "CARNES ROJAS RES", "CONFITERIA", "ARROZ", "AZUCAR Y EDULCORANTES"]
    tiendas = ["31, 32, 33, 36, 76, 89, 111, 137, 138, 140"]
    query_result = MagicMock()
    query_result.count.return_value = 100

    # Set up mocks
    mock_query_ventas_marcas_propias.return_value = query_ventas_marcas_propias(categorias=categorias, tiendas=tiendas)
    mock_logger = mocker.patch('forecasting_promocional.data_loaded.logging.getLogger')
    mock_logger.return_value = mocker.MagicMock()
    mock_spark.sql.return_value = query_result

    # Act
    loader = LoadDataVentas(spark=mock_spark, categorias=categorias, tiendas=tiendas)
    loader.load_data()

    # Assert
    mock_logger.return_value.info.assert_any_call(f"Sales from categories: {categorias} and stores: {tiendas}")
    mock_logger.return_value.info.assert_any_call(f"Dataframe ventas size: {query_result.count.return_value}")


if __name__ == "__main__":
    pytest.main()   
