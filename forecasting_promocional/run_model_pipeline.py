import json
import logging
import time
from databricks.sdk.runtime import spark
from forecasting_promocional.data_loaded import LoadDataVentas
from forecasting_promocional.data_preprocessing import DataPreprocessor
from forecasting_promocional.model_fit_and_predict import ModelFitAndResults
from forecasting_promocional.save_results import BetaSaver

def setup_logging(log_file):
    """
    Set up logging configuration.
    """
    logger = logging.getLogger("pipeline_logger")
    logger.setLevel(logging.INFO)

    # Create a file handler for logging
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setLevel(logging.INFO)

    # Create a logging format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Add the handlers to the logger
    if not logger.hasHandlers():  # Avoid adding multiple handlers in case of reruns
        logger.addHandler(file_handler)

    return logger

def load_config(config_path):
    """
    Load and update the configuration file.
    """
    with open(config_path, 'r') as file:
        config = json.load(file)

    return config


def run_pipeline():
    """
    Run the data processing and modeling pipeline.
    """
    start_time = time.time()

    log_file = 'pipeline_logs.txt'
    logger = setup_logging(log_file)

    # Log pipeline start
    logger.info("Pipeline execution started.")

    # global spark

    # Load configuration
    config = load_config("config.json")

    # Step 1: Data Ingestion
    logger.info("Step 1: Loading data from ventas and categories.")
    print("Step 1: Loading data from ventas and categories.")
    query_config = config['query_config'][0]
    categorias = query_config['categorias']
    tiendas = query_config['tiendas']

    loaddata = LoadDataVentas(spark=spark, categorias=categorias, tiendas=tiendas)
    df_ventas = loaddata.load_data()

    # Step 2: Data Preprocessing
    logger.info("Step 2: Processing the data for modeling.")
    print("Step 2: Processing the data for modeling.")

    data_processing_config = config["preprocessing_config"][0]
    min_year = data_processing_config["min_year"]
    max_year = data_processing_config["max_year"]

    preprocessor = DataPreprocessor()
    df_preprocessed = preprocessor.data_preprocessing(df=df_ventas, min_year=min_year, max_year=max_year)

    # Step 3: Model Fit and Predict
    logger.info("Step 3: Fitting the models and making predictions.")
    print("Step 3: Fitting the models and making predictions.")

    config = load_config("config.json")
    logger.info(f"Model config loaded")

    modelfit = ModelFitAndResults(config)
    results_df = modelfit.model_fit_and_results(df_preprocessed)

    # Step 4: Save Results
    logger.info("Step 4: Saving the results.")
    print("Step 4: Saving the results.")
    
    beta_saver = BetaSaver(spark=spark, df_results=results_df)
    beta_saver.save_beta_coefficients()

    logger.info("Pipeline execution completed.")
    print("Pipeline execution completed.")

    end_time = time.time()
    execution_time = end_time - start_time
    logger.info(f"Total execution time: {execution_time} seconds")
    print(f"Tiempo de ejecución: {execution_time} segundos")

if __name__ == "__main__":
    run_pipeline()