
import logging
import sys
#from pyspark.sql import SparkSession
import os

import pyspark
from delta import *


# utils and helpers imports
from utils.utils import spark_inst

# etl methods import
from etl_stages.extract import extract
from etl_stages.transform import transform
from etl_stages.load import load


def exe_etl():
    """Entry script to run the ETL process."""
    
    logging.info("START the ETL pipeline run\n")
    try:
        # Initiate Spark 
        logging.info("Initiating Sparks instance")
        SPARK = spark_inst()
        logging.info("Sparks instance ready\n")  

       
        # Extract stage    
        try:
            logging.info(" Extracting from source ....")
            extracted_cust_orders_df,extracted_records_count  = extract(SPARK,"CSV","myetl_data/source/data.csv")
            extracted_cust_orders_df.show()
            logging.info(f" - Number of records extracted: {extracted_records_count}")
            logging.info(f" Finished extracting from source. \n")
        except Exception as ex:
            logging.error(f" ERROR while extracting from source. Details: {ex}\n")
            raise
        
        # Transform stage     
        try:
            logging.info(" Transforming ....")
            transformed_cust_orders_df,transformed_records_count  = transform(SPARK,extracted_cust_orders_df)
            transformed_cust_orders_df.show()
            logging.info(f" - Number of records transformed: {transformed_records_count}")
            logging.info(f" Finished transformation. \n")
        except Exception as tr:
            logging.error(f" ERROR while transforming. Details: {tr}\n")
            raise

        # Load stage     
        try:
            table = "myetl_data/sink/CustomerOrders"
            logging.info(" Loading to Delta Table ....")
            loaded_records_count  = load(transformed_cust_orders_df,table)
            logging.info(f" - Table {table} loaded. Number of records: {loaded_records_count}")
            logging.info(f" Finished loading. \n")
        except Exception as ld:
            logging.error(f" ERROR while loading. Details: {ld}\n")
            raise
        
        logging.info("FINISHED successfully the ETL pipeline run")
        
    except Exception as e:
        logging.error(f"ERROR occurred during the ETL pipeline run. \n Details: {e}")
        raise
    
    
if __name__ == '__main__':
    # set the logging 
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    # run the main script
    exe_etl()


