import logging
import ast
from pyspark.sql.types import (FloatType, MapType, StringType)
from pyspark.sql.functions import col, udf

# Define parse_betas as a standalone function for use in UDF
def parse_betas(betas_str):
    """Function to parse betas into dict format."""
    try:
        return ast.literal_eval(betas_str)
    except Exception:
        return None  # Return None for failed parsing (logging handled after)

class BetaSaver:
    def __init__(self, spark, df_results):
        self.df_results = df_results
        self.spark = spark
        
        # Set logging level for Spark
        logging.getLogger("py4j").setLevel(logging.ERROR)

    def save_beta_coefficients(self):
        """Parse betas and insert into the table."""
        # Define UDF for parsing betas
        logging.info("Starting to parsing betas")
        parse_betas_udf = udf(parse_betas, MapType(StringType(), FloatType()))
        
        # Parse betas column and filter out rows with null betas
        df_parsed_betas = self.df_results.withColumn("betas", parse_betas_udf(col("betas")))
        df_parsed_betas = df_parsed_betas.filter(col("betas").isNotNull())

        # Optionally repartition the DataFrame for parallel processing
        df_parsed_betas = df_parsed_betas.repartition(col("sk_material"), col("sk_tienda"))

        # Insert the processed data into the target table
        logging.info("Starting to insert data into the table 'prod.df_betas'")
        try:
            df_parsed_betas.createOrReplaceTempView("df_betas_temp")
            self.spark.sql("INSERT INTO prod.df_betas SELECT * FROM df_betas_temp")
            self.spark.sql("OPTIMIZE prod.df_betas ZORDER BY (model_id, sk_material, sk_tienda)")
            logging.info("Data has been inserted into the table 'prod.df_betas'.")
        except Exception as e:
            logging.error(f"Error inserting data into 'dev.df_betas_delta': {e}")
            return

       
        max_model_id_df = self.spark.sql("SELECT MAX(model_id) as max_model_id FROM prod.df_betas")
        max_model_id = max_model_id_df.collect()[0]['max_model_id']
        logging.info(f"Last model if on 'prod.df_betas' after insert: {max_model_id}")
