project_id = "spark-learning-43150"
player_src_path_prefix = "player/"
src_bucket = "football_src_data"
process_bucket ="football_analysis_bucket"
process_file_prefix = "pyspark/bronze/bronze_process_file_log/"


from datetime import datetime

## bronze (player)
bronze_layer_player_loc = 'gs://football_analysis_bucket/pyspark/bronze/player/'
bronze_layer_player_loction = f"{bronze_layer_player_loc}player_{datetime.now().strftime('%Y%m%d')}"

## silver(player)
silver_layer_player_loc = 'gs://football_analysis_bucket/pyspark/silver/player/'
silver_layer_player_loction = f"{silver_layer_player_loc}player_{datetime.now().strftime('%Y%m%d')}"

##golden(player)
temp_dataproc_bucket= 'earthquake-dp_temp_bk'
golden_layer_player_loc = f'{project_id}.football_database.player_tbl'


