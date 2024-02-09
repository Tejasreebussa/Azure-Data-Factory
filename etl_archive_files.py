# Databricks notebook source
import datetime
import pytz
from pyspark.sql.functions import *

# COMMAND ----------

source_folder_loc = "/mnt/datalake/UKG/Adf"
archive_folder_loc = "/mnt/datalake/UKG/Adf_Archived"

# COMMAND ----------

def transform_load_files(source_path):
  df = spark.read\
            .format("csv")\
            .option("header",True)\
            .load(source_path)
  
  select_cols = ["Location","EPIC"]
  table_name = source_path[source_path.rfind("/")+1:]
  
  transformed_df = df.select(*select_cols)\
                     .withColumn("row_insert_tsp",lit(datetime.datetime.now()))
    
  transformed_df.write\
                .format("delta")\
                .saveAsTable(table_name)
  

