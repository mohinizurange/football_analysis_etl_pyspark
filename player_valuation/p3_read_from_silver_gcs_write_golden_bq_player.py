from google.cloud import storage
import os
import config
from utility import Utils
from pyspark.sql import SparkSession
from datetime import datetime
from google.cloud import bigquery

## define bq schema
bq_schema = {
    'fields': [
        {'name': 'date', 'type': 'DATE'},
        {'name': 'datetime', 'type': 'DATE'},
        {'name': 'dateweek', 'type': 'DATE'},
        {'name': 'player_id', 'type': 'INTEGER'},
        {'name': 'current_club_id', 'type': 'INTEGER'},
        {'name': 'market_value', 'type': 'INTEGER'},
        {'name': 'player_club_domestic_competition_id', 'type': 'STRING'}
    ]
}
if __name__=="__main__":
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\Mohini Data Science\FN_earthquake_ingestion_dataflow_parquet_dev\spark-learning-43150-3d588125392c.json"
    ## create sparksession
    spark = SparkSession.builder.master('local[*]')\
        .appName('football_etl_pipeline')\
       .getOrCreate()

    spark.conf.set("temporaryGcsBucket", config.temp_dataproc_bucket)

    ## create utils class obj
    utils_obj = Utils()

    ## call function readFromGCS  for read data from silver layer(GCS)
    silver_df =utils_obj.readFromGCS(spark,config.silver_layer_player_loction)
    silver_df.show()

    ## call function writeToBQ for write data into golden layer (bq)
    utils_obj.writeToBQ(silver_df,config.golden_layer_player_loc,bq_schema)