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

visa_df = spark.read.option("header",True).csv("/opt/workspace/data/i94visa_df.csv")
visa_df = visa_df.withColumnRenamed("Visa_Code","visa_code")\
                .withColumnRenamed("Reason","reason")
visa_df.write.mode("overwrite").option("header","true").parquet("s3a://ohmohmprde/" + "df_i94visa")