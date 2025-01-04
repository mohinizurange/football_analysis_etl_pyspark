from google.cloud import storage
import os
import config
from utility import Utils
from pyspark.sql import SparkSession
from datetime import datetime

if __name__=="__main__":
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\Mohini Data Science\FN_earthquake_ingestion_dataflow_parquet_dev\spark-learning-43150-3d588125392c.json"
    ## create sparksession
    spark = SparkSession.builder.master('local[*]')\
        .appName('football_etl_pipeline')\
       .getOrCreate()

    ## create utils class obj
    utils_obj = Utils()

    ## call function readFromGCS  for read data from bronze layer(GCS)
    bronze_raw_df =utils_obj.readFromGCS(spark, config.bronze_layer_player_loction)

    ## call function writeToGCS for write data into silver layer (GCS)
    utils_obj.writeToGCS(bronze_raw_df,config.silver_layer_player_loction)