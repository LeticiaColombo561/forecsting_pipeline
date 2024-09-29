import logging
from pyspark.sql import DataFrame
from schemas.schemas import *
from fp_functions.preprocess_data import (
    filter_sales, fix_data_errors, add_price_columns, fillna_with_next_or_previous, 
    monthly_std_dev_imputation_with_nearest, precio_ratio, fill_dates, 
    get_interest_columns, fill_holidays, add_date_features_pandas
)

class DataPreprocessor:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)

    def data_preprocessing(self, df, min_year, max_year) -> DataFrame:
        """
        Perform data preprocessing on the input DataFrame.

        Parameters:
        df (DataFrame): Input DataFrame.

        Returns:
        DataFrame: Processed DataFrame.
        """
        # calculate total sales by desc_material
        # total_sales = df.agg(
        #     F.round(F.sum("mto_venta_neta"), 0).alias("sum_mto_venta_neta"),
        #     F.round(F.sum("mto_venta_neta_promocion"), 0).alias("sum_mto_venta_neta_promocion")
        #     )

        # Filter sk_material and sk_tienda with sales in the last 30 days and at least 90 days of sales
        df_preprocessed = df.groupby(["sk_material", "sk_tienda"]).applyInPandas(
            filter_sales, 
            schema=schema_df_filtered)
        logging.info(f"Filtered sk_material and sk_tienda with sales in the last 30 days and at least 90 days of sales")
        print(f"Filtered sk_material and sk_tienda with sales in the last 30 days and at least 90 days of sales")
        
        # Fix data errors
        df_preprocessed = df_preprocessed.groupby(["sk_material", "sk_tienda"]).applyInPandas(
            fix_data_errors, 
            schema=schema_df_filtered)
        logging.info(f"Fixed data errors")
        print(f"Fixed data errors")

        # Add price columns
        df_preprocessed = df_preprocessed.groupby(["sk_material", "sk_tienda"]).applyInPandas(
            add_price_columns, 
            schema=schema_df_price)
        logging.info(f"Added price columns")
        print(f"Added price columns")

        # Fill missing values with the next or previous value
        df_preprocessed = df_preprocessed.groupby(["sk_material", "sk_tienda"]).applyInPandas(
            fillna_with_next_or_previous, 
            schema=schema_df_price)
        logging.info(f"Filled missing values with the next or previous value")
        print(f"Filled missing values with the next or previous value")

        # Impute outliers
        df_preprocessed = df_preprocessed.groupby(["sk_material", "sk_tienda"]).applyInPandas(
            monthly_std_dev_imputation_with_nearest,
            schema=schema_df_price)
        logging.info(f"Imputed outliers of precio_regular and precio_final")

        print(f"Imputed outliers of precio_regular and precio_final")

        # Calculate the price ratio
        df_preprocessed = df_preprocessed.groupby(["sk_material", "sk_tienda"]).applyInPandas(
            precio_ratio, 
            schema=schema_df_precio_ratio)
        logging.info(f"Calculated the price ratio")

        print(f"Calculated the price ratio")

        # Fill missing dates
        df_preprocessed = df_preprocessed.groupby(["sk_material", "sk_tienda"]).applyInPandas(
            fill_dates, 
            schema=schema_df_precio_ratio)
        logging.info(f"Filled missing dates")

        print(f"Filled missing dates")

        # total_sales_after = df_preprocessed.agg(
        #     F.round(F.sum("mto_venta_neta"), 0).alias("sum_mto_venta_neta_after"),
        #     F.round(F.sum("mto_venta_neta_promocion"), 0).alias("sum_mto_venta_neta_promocion_after")
        #     )
        
        # Get the columns of interest
        df_preprocessed = df_preprocessed.groupby(["sk_material", "sk_tienda"]).applyInPandas(
            get_interest_columns, 
            schema=schema_df_selected)
        logging.info(f"Selected columns of interest")

        print(f"Selected columns of interest")

        # Fill holidays
        df_preprocessed = df_preprocessed.groupby(["sk_material", "sk_tienda"]).applyInPandas(
            lambda df: fill_holidays(
               df,
               min_year=min_year,
               max_year=max_year
            ), 
            schema=schema_df_holiday)
        logging.info(f"Add holidays columns")

        print(f"Add holidays columns")

        # Add date features
        df_preprocessed = df_preprocessed.groupby(["sk_material", "sk_tienda"]).applyInPandas(
            add_date_features_pandas, 
            schema=schema_df_dates)
        logging.info(f"Added date features")

        print(f"Added date features")

        # % of sales lost
        # total_sales_after_values = total_sales_after.select(
        #      "sum_mto_venta_neta_after", "sum_mto_venta_neta_promocion_after"
        #      ).collect()[0]
        # total_sales_values = total_sales.select(
        #     "sum_mto_venta_neta", "sum_mto_venta_neta_promocion"
        #     ).collect()[0]
        # total_sales_neta_lost = 100 - round((total_sales_after_values[0] / total_sales_values[0]) * 100, 0)
        # total_sales_neta_promo_lost = 100 - round((total_sales_after_values[1] / total_sales_values[1]) * 100, 0)
        # total_sales.show()
        # total_sales_after.show()
        # logging.info("Sales lost percentage venta neta:", total_sales_neta_lost, "%",
        #       "Sales lost percentage venta neta promo:", total_sales_neta_promo_lost, "%")
        
        return df_preprocessed