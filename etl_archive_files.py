# Databricks notebook source
import datetime
import pytz
from pyspark.sql.functions import *

# COMMAND ----------

source_folder_loc = "/mnt/datalake/UKG/Adf"
archive_folder_loc = "/mnt/datalake/UKG/Adf_Archived"

