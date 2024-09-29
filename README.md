# Forecasting de Productos por Categoría y Tienda

## Descripción

Este proyecto tiene como objetivo realizar el forecasting de ventas para diferentes productos en distintas categorías y tiendas. El ajuste de los modelos se realiza utilizando una regresión log-log. El pipeline del proyecto incluye tres etapas principales: preprocesamiento de datos, ajuste y predicción del modelo, y guardado de resultados.

## Estructura del Proyecto

El proyecto está dividido en los siguientes scripts:

1. **data_ingestion.py**:

   - **Función**: Se encarga de la ingesta de datos desde una base de datos de Spark. Extrae información sobre ventas y materiales, filtra según las categorías y tiendas especificadas, y guarda los resultados en archivos CSV. También genera archivos con la relación de categorías y materiales.
   - **Entradas**:
     fecha_inicio: Fecha de inicio en formato YYYYMMDD.
     fecha_fin: Fecha de fin en formato YYYYMMDD.
     categorias: Lista de categorías de productos.
     tiendas: Lista de identificadores de tiendas.
     save_path: Ruta donde se guardarán los archivos CSV generados.
   - **Salidas**:
     df_venta_diaria.csv: Datos de ventas diarios.
     df_sk_categoria.csv: Relación de categorías.
     df_sk_material.csv: Relación de materiales por tienda.

2. **data_preprocessing.py**:

   - **Función**: Realiza el preprocesamiento de los datos, incluyendo la creación de variables lag, transformaciones logarítmicas, y preparación de los conjuntos de entrenamiento y prueba.
   - **Entradas**:
     - `df_sk_categoria.csv`: Archivo que contiene la relación de categorías y productos.
     - `data_ventas.csv`: Archivo con los datos de ventas diarios.
     - Fechas de corte para separar los datos de entrenamiento y prueba.
   - **Salidas**: Archivos CSV con los datos preprocesados.

3. **model_fit_and_predict.py**:

   - **Función**: Ajusta el modelo de regresión log-log utilizando los datos preprocesados y realiza predicciones de ventas.
   - **Entradas**:
     - `model_config.json`: Archivo de configuración con las definiciones de los modelos, incluyendo las columnas de características, lags, transformaciones logarítmicas, y parámetros del modelo.
   - **Salidas**: Archivo con las predicciones y métricas de rendimiento.

4. **save_results.py**:

   - **Función**: Guarda los coeficientes (betas) de los modelos ajustados y otras métricas importantes en archivos de salida para su posterior análisis.
   - **Entradas**:
     - `df_sk_categoria.csv`: Ruta al archivo de categorías.
     - `results`: Directorio donde se guardarán los resultados.
   - **Salidas**: Archivos con los coeficientes y métricas del modelo.

5. **run_pipeline.py**:
   - **Función**: Ejecuta los tres scripts anteriores en secuencia, utilizando un archivo de configuración (`launch_config.json`) que define los parámetros necesarios para cada script.
   - **Entradas**:
     - `launch_config.json`: Archivo de configuración para todo el pipeline.
   - **Salidas**: Resultados del preprocesamiento, predicción, y guardado de betas.

## Instalación

1. Clona el repositorio:

   ```bash
   git clone https://cencosud-fidelidad-rd@dev.azure.com/cencosud-fidelidad-rd/pe-sm-pricing-promocional/_git/Forecast
   cd cencosud-fidelidad-rd/pe-sm-pricing-promocional/Repo/Files/Forecast

   ```

2. Crear el eviroment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # Para Linux/Mac
   ```
3. Instalar project
   ```
   poetry install
   ```
4. Ejecuta el pipeline:
   ```bash
   python run_pipeline.py --config launch_config.json
   ```
