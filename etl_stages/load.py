
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

import pyspark
from delta import *

def load( df, location: str):
    """ Reads a dataframe and output spark delta table to the location specified """
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df.write.format("delta") \
        .option("overwriteSchema", "true") \
        .partitionBy("Product") \
        .mode("overwrite") \
        .save(location)
        
    count = df.count()
    # Stop SparkSession
    spark.stop()       
    return count