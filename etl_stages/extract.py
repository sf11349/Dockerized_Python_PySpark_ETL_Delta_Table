from pyspark.sql import SparkSession

def extract(spark: SparkSession, type: str, source: str):
    """ Reads an input and returns a dataframe & a count integer """
    if type == "CSV":
        output_df = spark.read.format("CSV").options(header=True, inferSchema=True).load(source)
        records_count = output_df.count()
        return output_df,records_count
    else:
        return None, None
