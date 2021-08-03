import configparser
import os
import pandas as pd
import sys

import pyspark
from pyspark.sql.functions import isnan, when, count, col, isnull, avg, round, udf, to_date
from pyspark.sql.types import DateType
from datetime import datetime, timedelta

config = configparser.ConfigParser()
config.read_file(open('/opt/workspace/dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

from pyspark.sql import SparkSession 
spark = SparkSession.builder\
        .config("spark.executor.memory", "4g")\
        .config("spark.executor.cores", "1")\
        .config("spark.hadoop.fs.s3a.access.key", KEY)\
        .config("spark.hadoop.fs.s3a.secret.key", SECRET)\
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .enableHiveSupport().getOrCreate()

def spark_shape(self):
    return (self.count(), len(self.columns))
pyspark.sql.dataframe.DataFrame.shape = spark_shape
# https://stackoverflow.com/questions/39652767/how-to-find-the-size-or-shape-of-a-dataframe-in-pyspark

def check_size(df_input):
    count_row = 0
    df_row = {}
    for name, df in df_input.items():
        row, col = df.shape()
        count_row = count_row + row
        print(name, row, col)
        df_row[name] = row
    print(count_row)

def sas_date_to_date(sas_date):
    if (sas_date):
        init_date = datetime(1960, 1, 1)
        return init_date + timedelta(days=sas_date)
    return None
udf_sas_date_to_date = udf(lambda x: sas_date_to_date(x), DateType())

def yyyyMMdd_date_to_date(date):
    try:
        return to_date(date, 'yyyyMMdd')
    except ValueError as e:
        return None
    
def MMddyyyy_date_to_date(date):
    try:
        today = datetime.now().today().strftime('%Y-%m-%d')
        return when( (to_date(date, 'MMddyyyy') > "1990-01-01") & (to_date(date, 'MMddyyyy') < today), to_date(date, 'MMddyyyy')).otherwise(None)
#https://robertjblackburn.com/how-to-clean-bad-dates-moving-to-spark-3/
    except ValueError as e:
        return None

month = int(sys.argv[1])
year  = sys.argv[2][2:4]
months = ['jan', 'feb', 'mar','apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
month_str = months[month-1]
d = {}
# https://stackoverflow.com/questions/30635145/create-multiple-dataframes-in-loop/30638956

d[month_str+year] = spark.read.format('com.github.saurfang.sas.spark').load(f'/opt/workspace/data/i94_{month_str}{year}_sub.sas7bdat')

d[month_str+year] = d[month_str+year].select("cicid", "i94yr", "i94mon", "i94cit", "i94res"\
                         ,"i94port", "arrdate", "i94mode", "i94addr"\
                        ,"depdate",  "i94visa",  "dtadfile", "entdepa"\
                        , "entdepd", "entdepu", "matflag", "biryear"\
                        ,"dtaddto", "gender","airline","admnum", "fltno","visatype")

d[month_str+year] = d[month_str+year].withColumn("arrdate",udf_sas_date_to_date(d[month_str+year].arrdate))\
                    .withColumn("depdate",udf_sas_date_to_date(d[month_str+year].depdate))\
                    .withColumn("dtadfile", yyyyMMdd_date_to_date(d[month_str+year].dtadfile))\
                    .withColumn("dtaddto", MMddyyyy_date_to_date(d[month_str+year].dtaddto))\

d[month_str+year] = d[month_str+year].withColumn("i94yr_partition",d[month_str+year].i94yr)\
                    .withColumn("i94mon_partition", d[month_str+year].i94mon)\
                    .withColumn("arrdate_partition", d[month_str+year].arrdate)

d[month_str+year].write.partitionBy("i94yr_partition","i94mon_partition","arrdate_partition").mode("append").parquet("s3a://ohmohmprde/" + "df_immigration")
