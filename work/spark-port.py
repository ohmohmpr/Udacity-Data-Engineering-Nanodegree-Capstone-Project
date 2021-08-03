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

port_df = spark.read.option("header",True).csv("/opt/workspace/data/i94port_df.csv")
port_df = port_df.withColumnRenamed("Port_pode","port_code")\
                .withColumnRenamed("Port","port")
port_df.write.mode("overwrite").option("header","true").parquet("s3a://ohmohmprde/" + "df_i94port")