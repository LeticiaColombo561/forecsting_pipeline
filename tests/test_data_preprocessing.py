import pytest
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from forecasting_promocional.data_preprocessing import DataPreprocessor
from schemas.schemas import schema_df_ventas, schema_df_dates
from schemas.names import (DfVentasNames)


class TestDataPreprocessing:
    """ Test the data preprocessing module. """

    @pytest.fixture(autouse=True)
    def setUp(self):
        """Set up test data and objects."""

    def generate_sample_data(self):
        """Create a sample DataFrame for testing"""
        np.random.seed(0) 
        n_rows = 380

        years = np.random.choice([2023, 2024], size=n_rows)
        months = np.random.choice(range(1, 13), size=n_rows)
        days = np.random.choice(range(1, 31), size=n_rows)

        # Create valid dates by ensuring correct day counts for each month
        valid_dates = []
        for year, month, day in zip(years, months, days):
            try:
                date = pd.Timestamp(year=year, month=month, day=day)
            except ValueError:
                # Skip invalid dates
                continue
            valid_dates.append(date.strftime('%Y%m%d'))
        
        # Ensure we have enough valid dates
        if len(valid_dates) < n_rows:
            additional_dates_needed = n_rows - len(valid_dates)
            valid_dates += np.random.choice(valid_dates, size=additional_dates_needed).tolist()
    

        data = pd.DataFrame(
            {
            DfVentasNames.SK_DIA.value: valid_dates[:n_rows],
            DfVentasNames.SK_MES.value: np.random.randint(202312, 202409, size=n_rows),
            DfVentasNames.SK_MATERIAL.value: np.random.choice([1001, 1002], size=n_rows),
            DfVentasNames.SK_TIENDA.value: np.random.choice([10, 20], size=n_rows),
            DfVentasNames.DESC_MATERIAL.value: np.random.choice([
                "ASADO PEJERREY MITAD WONG PREMIUM",
                "PANES VARIOS X KG PPM"], size=n_rows),
            DfVentasNames.DESC_GRUPO_ARTICULO.value: np.random.choice([
                "RES NACIONAL CORTE PRIMARIO ASADOS",
                "PANES VARIOS VENTA"], size=n_rows),
            DfVentasNames.DESC_CATEGORIA.value: np.random.choice([
                "CARNES ROJAS RES",
                "PANADERIA"], size=n_rows),
            DfVentasNames.SK_CATEGORIA.value: np.random.randint(1, 20, size=n_rows),
            DfVentasNames.MTO_VENTA_NETA.value: np.round(np.random.uniform(20, 700, size=n_rows), 2),
            DfVentasNames.MTO_VENTA_NETA_PROMOCION.value: np.round(np.random.uniform(0, 300, size=n_rows), 2),
            DfVentasNames.CNT_VENTA.value: np.round(np.random.uniform(1, 200, size=n_rows), 3),
            DfVentasNames.CNT_VENTA_PROMOCION.value: np.round(np.random.uniform(0, 200, size=n_rows), 3),
            }
            )

        data[DfVentasNames.SK_MES.value] = data[DfVentasNames.SK_DIA.value].apply(lambda x: int(str(x)[:4]))
        data[DfVentasNames.MTO_VENTA_NETA_PROMOCION.value] = data[DfVentasNames.MTO_VENTA_NETA.value
                                                                  ] * np.random.uniform(0, 1, size=len(data))
        data[DfVentasNames.CNT_VENTA_PROMOCION.value] = data[DfVentasNames.CNT_VENTA.value
                                                             ] * np.random.uniform(0, 1, size=len(data))
        data[DfVentasNames.SK_DIA.value] = data[DfVentasNames.SK_DIA.value].astype(int)
    
        return data

    def test_data_preprocessing(self):
        min_year = 2023
        max_year = 2024


        df_ventas = self.generate_sample_data()
        spark = SparkSession.builder.getOrCreate()
        df_ventas = spark.createDataFrame(df_ventas, schema=schema_df_ventas)

        # Run the data preprocessing
        preprocessor = DataPreprocessor()
        df_processed = (
            preprocessor.data_preprocessing(df_ventas, min_year, max_year)
        )
        
        # Show unique sk_material and sk_tienda
        assert df_processed.select(F.col("sk_material")).distinct().count() == 2
        assert df_processed.select(F.col("sk_tienda")).distinct().count() == 1

        # Ensure no null values in precio_ratio or cnt_venta
        assert df_processed.filter(F.col("precio_ratio").isNull()).count() == 0
        assert df_processed.filter(F.col("cnt_venta").isNull()).count() == 0
        
        # Ensure max_precio ratio is 1 or less
        assert df_processed.agg(F.max("precio_ratio")).collect()[0][0] <= 1

        # Ensure columns are the same as schema_df_dates
        assert df_processed.schema == schema_df_dates


if __name__ == "__main__":
    pytest.main()