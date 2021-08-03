import configparser
import os
import pandas as pd

from pyspark.sql.types import IntegerType


config = configparser.ConfigParser()
config.read_file(open('/opt/workspace/dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

from pyspark.sql import SparkSession 
spark = SparkSession.builder\
        .config("spark.executor.memory", "2g")\
        .config("spark.executor.cores", "1")\
        .config("spark.hadoop.fs.s3a.access.key", KEY)\
        .config("spark.hadoop.fs.s3a.secret.key", SECRET)\
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .enableHiveSupport().getOrCreate()

mode_df = spark.read.option("header",True).csv("/opt/workspace/data/i94mode_df.csv")
mode_df = mode_df.withColumnRenamed("Transport_Code","transport_code")\
                .withColumnRenamed("Transport_Type","transport_type")
mode_df.write.mode("overwrite").option("header","true").parquet("s3a://ohmohmprde/" + "df_i94mode")