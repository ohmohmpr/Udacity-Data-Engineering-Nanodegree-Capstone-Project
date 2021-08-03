from airflow.operators.bash import BashOperator
from load_fact import LoadFactOperator
from stage_redshift import StageToRedshiftOperator
from data_quality import DataQualityOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from datetime import datetime
from sql_queries import SqlQueries

default_args = {
    "owner": "airflow", 
    "start_date": datetime(2016, 2, 1), 
    'end_date': datetime(2017, 2, 1)
}


dag = DAG(
    dag_id="load_fact_monthly2", 
    default_args=default_args, 
    schedule_interval="0 2 5 * *"
) ## At 02:00 on day-of-month 5.

month="{{prev_execution_date.month}}"
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


upload_local_to_s3_immigrations = BashOperator(
        task_id='local_to_s3_immigrations',
        bash_command='spark-submit --master spark://spark:7077 --name upload_parquet_immigration_s3 --jars "local:///opt/workspace/jars/hadoop-aws-3.1.2.jar,local:///opt/workspace/jars/spark-sas7bdat-3.0.0-s_2.12.jar,local:///opt/workspace/jars/parso-2.0.11.jar,local:///opt/workspace/jars/aws-java-sdk-bundle-1.11.271.jar" /opt/workspace/spark-immigration.py {{prev_execution_date.month}} {{prev_execution_date.year}}',
        dag=dag,
)
# https://stackoverflow.com/questions/53740554/import-airflow-variable-to-pyspark
# https://stackoverflow.com/questions/32217160/can-i-add-arguments-to-python-code-when-i-submit-spark-job

stage_immigrations_to_redshift = StageToRedshiftOperator(
    task_id='staging_immigrations',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    client_type_id='redshift',
    table='staging_immigrations',
    s3_bucket='ohmohmprde',
    s3_key="df_immigration/i94yr_partition=2016.0/i94mon_partition={{prev_execution_date.month}}",
    append_data=True,
)

load_immigrations_fact_table = LoadFactOperator(
    task_id='Load_immigrations_fact_table',
    dag=dag,
    sql=SqlQueries.immigration_fact_table_insert,
    target_table='fact_immigrations',
    target_columns='''cicid,
                        i94yr,
                        i94mon,
                        i94cit,
                        i94res,
                        i94port,
                        arrdate,
                        i94mode,
                        i94addr,
                        depdate,
                        i94visa,
                        dtadfile,
                        entdepa, 
                        entdepd,
                        entdepu, 
                        matflag,
                        biryear,
                        dtaddto,
                        gender,
                        airline, 
                        admnum,
                        fltno,
                        visatype
                    ''',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    client_type_id='redshift',
    append_data=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.immigration_fact_table_check_null,
    result=0,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> upload_local_to_s3_immigrations
upload_local_to_s3_immigrations >> stage_immigrations_to_redshift
stage_immigrations_to_redshift >> load_immigrations_fact_table
load_immigrations_fact_table >> run_quality_checks
run_quality_checks >> end_operator

