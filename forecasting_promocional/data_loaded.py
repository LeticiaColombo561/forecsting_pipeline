import logging
from fp_functions.df_query import query_ventas_marcas_propias

class LoadDataVentas:
    def __init__(self, spark, categorias, tiendas):
        self.spark = spark
        self.categorias = categorias
        self.tiendas = tiendas
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        self.query_ventas_marcas_propias = query_ventas_marcas_propias

    def load_data(self):
        """
        Execute the ingestion process for ventas marcas propias and sk_categoria.
        """
        # Query and load data for ventas marcas propias
        query_ventas = self.query_ventas_marcas_propias(categorias=self.categorias,
                                                        tiendas=self.tiendas)
        
        self.logger.info(f"Sales from categories: {self.categorias} and stores: {self.tiendas}")
        df_ventas = self.spark.sql(query_ventas)
        self.logger.info(f"Dataframe ventas size: {df_ventas.count()}")

        return  df_ventas
    