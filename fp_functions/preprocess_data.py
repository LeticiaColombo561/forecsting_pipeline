import pandas as pd
from pyspark.sql.types import *
import numpy as np
from datetime import datetime, timedelta
import logging

import holidays
from typing import List

yesterday = datetime.now() - timedelta(days=1)

def filter_sales(df: pd.DataFrame) -> pd.DataFrame:

    df['fecha'] = pd.to_datetime(df['sk_dia'].astype(str), format='%Y%m%d')
    df['mes'] = df['fecha'].dt.strftime('%Y%m')
    
    sk_material = df['sk_material'].unique()[0]
    sk_tienda = df['sk_tienda'].unique()[0]
    
    # Filtrar registros con ventas en los últimos 30 días desde ayer
    mask_30_days = (df['fecha'] > yesterday - timedelta(days=30))
    
    # Filtrar registros con menos de 90 días de ventas
    total_sales_days = (df['fecha'].count())
    mask_90_days = total_sales_days >= 90
    
    # Aplicar ambas condiciones: ventas en últimos 30 días y menos de 90 días con ventas
    if mask_30_days.any() and mask_90_days:
        cols = ['fecha', 'mes'] + [col for col in df.columns if col not in ['fecha', 'mes']]
        df = df[cols]
        return df
    else:
        if mask_30_days.any() == False:
            logging.info(f"sku_material- sk_tienda {sk_material}-{sk_tienda} skipped due to lack of sales in the last 30 days")
        else:
            logging.info(f"sku_material- sk_tienda {sk_material}-{sk_tienda} skipped due to less than 90 days of sales")
        return pd.DataFrame()


def fix_data_errors(df: pd.DataFrame) -> pd.DataFrame:
    # 1: If mto_venta_neta == mto_venta_neta_promocion & cnt_venta != cnt_venta_promocion, set cnt_venta_promocion = cnt_venta
    df.loc[
        (df["mto_venta_neta"] == df["mto_venta_neta_promocion"]) &
        (df["cnt_venta"] != df["cnt_venta_promocion"]), 
        "cnt_venta_promocion"
    ] = df["cnt_venta"]

    # 2: If mto_venta_neta < mto_venta_neta_promocion, sum both mto_venta_neta and cnt_venta
    mask_venta_neta_menor = df["mto_venta_neta"] < df["mto_venta_neta_promocion"]
    df.loc[mask_venta_neta_menor, "mto_venta_neta"] += df.loc[mask_venta_neta_menor, "mto_venta_neta_promocion"]
    df.loc[mask_venta_neta_menor, "cnt_venta"] += df.loc[mask_venta_neta_menor, "cnt_venta_promocion"]

    # 3: If mto_venta_neta_promocion == 0 & cnt_venta_promocion > 0, set cnt_venta_promocion = 0
    df.loc[
        (df["mto_venta_neta_promocion"] == 0) &
        (df["cnt_venta_promocion"] > 0), 
        "cnt_venta_promocion"
    ] = 0

    # 4: If mto_venta_neta == 0 & cnt_venta > 0, set cnt_venta = 0
    df.loc[
        (df["mto_venta_neta"] == 0) &
        (df["cnt_venta"] > 0), 
        "cnt_venta"
    ] = 0

    # 5: If mto_venta_neta_promocion > 0 & cnt_venta_promocion == 0, set mto_venta_neta_promocion = 0
    df.loc[
        (df["mto_venta_neta_promocion"] > 0) &
        (df["cnt_venta_promocion"] == 0), 
        "mto_venta_neta_promocion"
    ] = 0

    # 6: If mto_venta_neta > 0 & cnt_venta == 0, set mto_venta_neta = 0
    df.loc[
        (df["mto_venta_neta"] > 0) &
        (df["cnt_venta"] == 0), 
        "mto_venta_neta"
    ] = 0

    return df


def add_price_columns(df):
    """
    Adds sales columns to the DataFrame.

    Parameters:
    df (DataFrame): Input DataFrame.
    date_column (str): Name of the date column.
    date_format (str): Format of the date in the date_column.

    Returns:
    DataFrame: DataFrame with additional sales columns.
    """
    # Sort by date column (sorting isn't necessary in Spark as transformations don't retain order)
    df_prices = df.sort_values(by="fecha")

    # Calculate new columns
    df_prices["mto_venta_neta_regular"] = (df_prices["mto_venta_neta"] - df_prices["mto_venta_neta_promocion"]).round(2)
    df_prices["cnt_venta_regular"] = (df_prices["cnt_venta"] - df_prices["cnt_venta_promocion"]).round(2)
    
    df_prices["precio_final"] = (df_prices["mto_venta_neta"] / df_prices["cnt_venta"]).round(2)
    df_prices["precio_regular"] = (df_prices["mto_venta_neta_regular"] / df_prices["cnt_venta_regular"]).round(2)

    # Reorder columns
    cols = ['fecha', 'mes'] + [col for col in df_prices.columns if col not in ['fecha', 'mes']]
    df_prices = df_prices[cols]

    return df_prices

def fillna_with_next_or_previous(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rellena los valores nulos o ceros en una columna específica con el siguiente valor no nulo
    hacia adelante, y si no hay siguiente, con el anterior.

    Parameters:
    df (pd.DataFrame): El DataFrame que contiene los datos.
    col (str): El nombre de la columna a rellenar.

    Returns:
    pd.DataFrame: El DataFrame con los valores nulos o ceros rellenados.
    """
    
    # Reemplazar ceros, inf por NaN para tratarlos como valores faltantes
    df["precio_regular"] = df["precio_regular"].replace(0, np.nan)
    df["precio_regular"] = df["precio_regular"].replace([np.inf, -np.inf], np.nan)

    # Rellenar los NaN con el siguiente valor no nulo hacia adelante (ffill) o hacia atrás (bfill)
    df["precio_regular"] = df["precio_regular"].ffill().bfill()


    # Reemplazar ceros, inf  por NaN para tratarlos como valores faltantes
    df["precio_final"] = df["precio_final"].replace(0, np.nan)
    df["precio_regular"] = df["precio_regular"].replace([np.inf, -np.inf], np.nan)

    # Rellenar los NaN con el siguiente valor no nulo hacia adelante (ffill) o hacia atrás (bfill)
    df["precio_final"] = df["precio_final"].ffill().bfill()

    return df

def monthly_std_dev_imputation_with_nearest(df: pd.DataFrame) -> pd.DataFrame:
    """
    Imput values outside of 4 standard deviations from the mean with the nearest non-outlier value.

    Parameters:
    df (pd.DataFrame): Dataframe with the data.

    Returns:
    pd.DataFrame: DataFrame with corrected values.
    """
    # Calulate the monthly statistics
    col_to_imput =["precio_regular", "precio_final"]
    monthly_stats = df.groupby( "precio_final")["precio_final"].agg(['mean', 'std']).round(2)
    
    for col in col_to_imput:
        imput_col = df[col].copy()

        # Iter over the months
        for month, stats in monthly_stats.iterrows():
            mean = stats['mean']
            std_dev = stats['std']
            lower_bound = mean - 3 * std_dev
            upper_bound = mean + 3 * std_dev
            
            # Imputar values greater than 3 standard deviations above the mean
            mask_upper = (df["mes"] == month) & (df[col] > upper_bound)
            imput_col.loc[mask_upper] = imput_col.loc[mask_upper].bfill(limit=1).ffill(limit=1)
            imput_col.loc[mask_upper & (imput_col > upper_bound)] = upper_bound
            
            # Imputar valores lower than 3 standard deviations below the mean
            mask_lower = (df["mes"] == month) & (df[col] < lower_bound)
            imput_col.loc[mask_lower] = imput_col.loc[mask_lower].bfill(limit=1).ffill(limit=1)
            imput_col.loc[mask_lower & (imput_col < lower_bound)] = lower_bound

        # Update the column in the DataFrame
        df[col] = imput_col.round(2)
         
        # If precio_regular is greater than precio_final, set precio_regular = precio_final
        df.loc[df["precio_regular"] < df["precio_final"], "precio_regular"] = df["precio_final"]

    return df


def precio_ratio(df: pd.DataFrame) -> pd.DataFrame:
    df['precio_ratio'] = (df['precio_final'] / df['precio_regular']).round(4)
    return df



def fill_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing dates in a Pandas DataFrame and reorder columns.
    """
    # Convert 'sk_dia' to a proper date
    df['fecha'] = pd.to_datetime(df['sk_dia'].astype(str), format='%Y%m%d')
    
    # Extract sk_material and sk_tienda
    sk_material = df['sk_material'].unique()[0]
    sk_tienda = df['sk_tienda'].unique()[0]
    
    # Get the min and max date for each group
    min_fecha = df['fecha'].min()
    max_fecha = df['fecha'].max()
    
    # Create a complete range of dates
    all_dates = pd.date_range(start=min_fecha, end=max_fecha, freq='D')
    
    # Create a DataFrame with the full range of dates
    df_full = pd.DataFrame({
        'fecha': all_dates
    })
    
    # Add sk_material and sk_tienda to the date range DataFrame
    df_full['sk_material'] = sk_material
    df_full['sk_tienda'] = sk_tienda
    
    # Merge with the original DataFrame
    df_full = df_full.merge(df, on=['fecha', 'sk_material', 'sk_tienda'], how='left')
    
    # Fill missing values
    df_full['cnt_venta'] = df_full['cnt_venta'].fillna(0)
    df_full['cnt_venta_promocion'] = df_full['cnt_venta_promocion'].fillna(0)
    df_full['mto_venta_neta'] = df_full['mto_venta_neta'].fillna(0)
    df_full['mto_venta_neta_promocion'] = df_full['mto_venta_neta_promocion'].fillna(0)
    df_full['precio_ratio'] = df_full['precio_ratio'].fillna(1)
    
    # Add 'mes' column
    df_full['mes'] = df_full['fecha'].dt.strftime('%Y%m')
    
    # Reorder columns to have 'fecha' and 'mes' as the first two columns
    cols = ['fecha', 'mes'] + [col for col in df_full.columns if col not in ['fecha', 'mes']]
    df_full = df_full[cols]
    
    return df_full


def get_interest_columns(df):
    df = df[["sk_material", "sk_tienda", "fecha", "mes", "cnt_venta", "precio_ratio"]]
    return df


def peru_holiday(years: List[int]) -> pd.DataFrame:
    """
    Genera un DataFrame con las fechas de los feriados nacionales en Perú para los años
    especificados. Y las fechaa de inicio y fin de la semana previa al feriado.

    Parameters:
    years (List[int]): Lista de años para los cuales se desean obtener los feriados.

    Returns:
    pd.DataFrame: DataFrame con la fecha de inicio y fin de la semana previa de cada feriado.
    """
    peru_holidays = holidays.Peru(years=years, language="es")
    holidays_df = pd.DataFrame(list(peru_holidays.items()), columns=["fecha_fin", "week_holiday"])
    # fecha inicio shift 7 days
    holidays_df["fecha"] = pd.to_datetime(holidays_df["fecha_fin"], format="%Y-%m-%d")
    holidays_df["fecha_inicio"] = pd.to_datetime(
        holidays_df["fecha_fin"] - pd.DateOffset(days=7), format="%Y-%m-%d"
    )
    holidays_df["fecha_fin"] = pd.to_datetime(
        holidays_df["fecha_fin"] - pd.DateOffset(days=1), format="%Y-%m-%d"
    )
    # replace space by _ and lower case
    holidays_df["week_holiday"] = holidays_df["week_holiday"].str.lower().str.replace(" ", "_")
    holidays_df = holidays_df[["week_holiday", "fecha", "fecha_inicio", "fecha_fin"]]

    return holidays_df


def fill_holidays(df, min_year, max_year):
    """
    Llena el DataFrame con las columnas de feriados nacionales en Perú.
    
    Parameters:
    df (pd.DataFrame): DataFrame a rellenar con la dummy de la semana previa al feriado.
    holidays_df (pd.DataFrame): DataFrame con las fechas de los feriados nacionales en Perú.
    
    Returns:
    pd.DataFrame: DataFrame con las columnas de feriados nacionales en Perú indicando
    si la fecha corresponde o no a la semana previa al feriado.
    """
    years = list(range(min_year, max_year + 1))
    holidays_df = peru_holiday(years)

    # Convert 'fecha_inicio' and 'fecha_fin' to UTC timezone
    holidays_df["fecha_inicio"] = pd.to_datetime(holidays_df["fecha_inicio"], utc=True)
    holidays_df["fecha_fin"] = pd.to_datetime(holidays_df["fecha_fin"], utc=True)

    # Ensure 'fecha' in df is timezone-aware
    df['fecha'] = pd.to_datetime(df['fecha'], utc=True)

    for _, row in holidays_df.iterrows():
        holiday_name = row["week_holiday"]
        fecha_inicio = row["fecha_inicio"]
        fecha_fin = row["fecha_fin"]

        # If the column doesn't exist, create it
        if holiday_name not in df.columns:
            df[holiday_name] = False

        # Perform the comparison between timezone-aware datetime values
        df.loc[df["fecha"].between(fecha_inicio, fecha_fin), holiday_name] = True

    return df


def week_of_month(fecha: pd.Series) -> pd.Series:
    """
    Calculate the week of the month for a Pandas column of dates.
    """
    first_day_of_month = fecha - pd.to_timedelta(fecha.dt.day - 1, unit='d')
    dom = fecha.dt.day
    adjusted_dom = dom + first_day_of_month.dt.weekday
    return (adjusted_dom // 7 + 1)

def add_date_features_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds date-related features to the dataframe and creates dummy variables for 'day_of_week', 
    'week_of_month', and 'month', then drops the original columns.
    """
    # Convert 'fecha' to datetime if it's not already
    df['fecha'] = pd.to_datetime(df['fecha'])

    # Add basic date features
    df['weekend'] = df['fecha'].dt.weekday.isin([5, 6])  # Saturday=5, Sunday=6
    df['day_of_week'] = df['fecha'].dt.dayofweek + 1      # To match SQL-like 1=Monday, 7=Sunday
    df['week_of_month'] = week_of_month(df['fecha'])
    df['month'] = df['fecha'].dt.month
    df['year'] = df['fecha'].dt.year

    # Create dummy variables for 'day_of_week', 'week_of_month', and 'month'
    df = pd.get_dummies(df, columns=['weekend', 'day_of_week', 'week_of_month', 'month'], drop_first=True)

    # Ensure the DataFrame contains all expected dummy columns as per the schema
    expected_columns = ['weekend_True', 
                        'day_of_week_2', 'day_of_week_3', 'day_of_week_4', 'day_of_week_5', 
                        'day_of_week_6', 'day_of_week_7', 'week_of_month_2', 'week_of_month_3', 
                        'week_of_month_4', 'week_of_month_5', 'week_of_month_6',
                        'month_2', 'month_3', 'month_4', 'month_5', 'month_6', 'month_7', 
                        'month_8', 'month_9', 'month_10', 'month_11', 'month_12']
    
    for col in expected_columns:
        if col not in df.columns:
            df[col] = False  # Add missing columns with default value False

    # Drop unnecessary columns
    df = df.drop(columns=['mes', 'year'])

    return df

