import pyspark
from delta import *


def spark_inst():
    """ Method for creating a spark instance """
    try:
        builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()     
        
        return spark
        
    except Exception as sp_error:
        raise sp_error