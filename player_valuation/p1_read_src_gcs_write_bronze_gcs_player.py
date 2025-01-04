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
    src_bucket_name = config.src_bucket
    folder_name = config.player_src_path_prefix

    ### for process file name lst
    process_bucket_name = config.process_bucket
    process_file_prefix= config.process_file_prefix
    process_file_log_path = process_file_prefix + "player_bronze.txt"
    # print(process_file_log_path) ##pyspark/bronze/bronze_process_file_log/player_bronze.txt

    ## create utils class obj
    utils_obj = Utils()

    ##call function newFileIdentify
    player_new_file_lst =utils_obj.newFileIdentify(project_id,src_bucket_name,folder_name,process_bucket_name,process_file_log_path)
    # print(player_new_file_lst)

    for file_nm in player_new_file_lst:
        raw_file = f"gs://{src_bucket_name}/{folder_name}{file_nm}"
        print('raw file path',raw_file)
        # print(raw_file)
        ## call function readFromGCS  for read data from src_location (GCS)
        raw_df = spark.read.csv(raw_file)
        raw_df=utils_obj.readFromGCS(spark, raw_file, 'csv')
        raw_df.show()


        ## call function writeTOGCS for write raw data into bronze layer
        utils_obj.writeToGCS(raw_df, config.bronze_layer_player_loction)



