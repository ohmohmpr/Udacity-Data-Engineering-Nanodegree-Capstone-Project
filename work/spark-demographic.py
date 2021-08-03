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

demographics_df = spark.read.option("header",True).options(delimiter=';').csv("/opt/workspace/data/us-cities-demographics.csv") 
demographics_df_main = demographics_df.select("State Code", "State", "City", "Male Population", "Female Population", "Total Population", "Number of Veterans", "Foreign-born")
demographics_df_main = demographics_df_main.dropDuplicates()
demographics_df_main.count() ## select only city and state 
demographics_df_main = demographics_df_main.withColumn("Male Population", demographics_df_main["Male Population"].cast(IntegerType()))\
                                        .withColumn("Female Population", demographics_df_main["Female Population"].cast(IntegerType()))\
                                        .withColumn("Total Population", demographics_df_main["Total Population"].cast(IntegerType()))\
                                        .withColumn("Number of Veterans", demographics_df_main["Number of Veterans"].cast(IntegerType()))\
                                        .withColumn("Foreign-born", demographics_df_main["Foreign-born"].cast(IntegerType()))
demographics_df_main = demographics_df_main.groupBy("State Code").sum("Total Population","Male Population", "Female Population", "Number of Veterans",  "Foreign-born")
demographics_df_main = demographics_df_main.withColumnRenamed("State Code","state_code")\
                                        .withColumnRenamed("State","state")\
                                        .withColumnRenamed("City","city")\
                                        .withColumnRenamed("sum(Total Population)","total_population")\
                                        .withColumnRenamed("sum(Male Population)","male_population")\
                                        .withColumnRenamed("sum(Female Population)","female_population")\
                                        .withColumnRenamed("sum(Number of Veterans)","number_of_veterans")\
                                        .withColumnRenamed("sum(Foreign-born)","foreign_born")
demographics_df_race = demographics_df.select("State Code", "City", "State", "Race", "Count")
demographics_df_race = demographics_df_race.withColumnRenamed("State Code","state_code_r")\
                                        .withColumnRenamed("City","city_r")\
                                        .withColumnRenamed("State","state_r")\
                                        .withColumnRenamed("Race","race")\
                                        .withColumnRenamed("Count","count")
demographics_df_race = demographics_df_race.withColumn("count", demographics_df_race["count"].cast(IntegerType()))
demographics_df_race = demographics_df_race.groupBy("state_code_r").pivot("race").sum("count")
demographics_df_by_state = demographics_df_main.join(demographics_df_race,demographics_df_main["state_code"] ==  demographics_df_race["state_code_r"],"inner")
demographics_df_by_state = demographics_df_by_state.drop("state_code_r")
demographics_df_by_state = demographics_df_by_state.withColumnRenamed("American Indian and Alaska Native","american_indian_and_alaska_native")\
                                        .withColumnRenamed("Asian","asian")\
                                        .withColumnRenamed("Black or African-American","black_or_african_american")\
                                        .withColumnRenamed("Hispanic or Latino","hispanic_or_latino")\
                                        .withColumnRenamed("White","white")
demographics_df_by_state = demographics_df_by_state.sort(demographics_df_by_state["state_code"].asc())
demographics_df_by_state.write.mode("overwrite").option("header","true").parquet("s3a://ohmohmprde/" + "df_demo")