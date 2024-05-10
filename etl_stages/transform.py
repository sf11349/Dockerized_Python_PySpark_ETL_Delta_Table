
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def transform(spark: SparkSession, df: DataFrame):
    """ Reads an dataframe and returns a transfomed dataframe and counts integer """
    transf_df = df.withColumn("TotalOrderAmount", df.Quantity*df.Price)
    return transf_df, df.count()

