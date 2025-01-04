from google.cloud import storage
import os
import config

class Utils:
    def newFileIdentify(self,project_id,src_bucket_name,folder_name,process_bucket_name,process_file_log_path):
        # os.environ[
        #     'GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\Mohini Data Science\FN_earthquake_ingestion_dataflow_parquet_dev\spark-learning-43150-3d588125392c.json"
        ##initialize the client
        storage_client = storage.Client(project=project_id)
        blobs_name = storage_client.list_blobs(bucket_or_name=src_bucket_name, prefix=folder_name)
        # print(blobs_name)
        total_file_name_lst = []
        for blob in blobs_name:
            prefix = blob.name
            filename = prefix.split('/')[-1]
            # print(filename)'',player_valuations.csv
            if filename != '':
                total_file_name_lst.append(filename)
        print(f"total file names lst : {total_file_name_lst}")

        ### for process file name lst
        # print(process_file_log_path) ##pyspark/bronze/bronze_process_file_log/player_bronze.txt
        try:
            process_bucket_name_obj = storage_client.bucket(process_bucket_name)
            blob = process_bucket_name_obj.blob(process_file_log_path)
            # print(blob)
            # Read the file content
            file_content = blob.download_as_text()
            # print(file_content,type(file_content)) ##test.csv,test2.csv <class 'str'>
            process_file_lst = file_content.split(',')
            # print(process_file_lst) ##['test.csv', 'test2.csv']

        except:
            process_file_lst = []

        print(f"process file lst : {process_file_lst}")

        new_file_name = list(set(total_file_name_lst) - set(process_file_lst))
        print(f"new_files are :{new_file_name}")
        return new_file_name

    def writeToGCS(self,df,location,frmat='parquet'):
        df.write.format(frmat).save(location)

    def readFromGCS(self,spark,location,frmat='parquet'):
        if frmat=='csv':
            df = spark.read.csv(location,header = True,inferSchema = True)
            return df
        else:
            df = spark.read.format(frmat).load(location)
            return df
    def writeToBQ(self,df,tbl_location,bq_schema):
        df.write.format('bigquery') \
            .option('table', tbl_location) \
            .option('schema', bq_schema) \
            .option('writeDisposition', 'WRITE_TRUNCATE') \
            .option('createDisposition', 'CREATE_IF_NEEDED') \
            .mode('overwrite') \
            .save()





