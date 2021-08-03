from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from stage_redshift import StageToRedshiftOperator
from load_dimension import LoadDimensionOperator
from data_quality import DataQualityOperator
from data_quality_check_duplicate import DataQualityCheckDuplicateOperator
from airflow import DAG
from datetime import datetime
from sql_queries import SqlQueries

default_args = {
    "owner": "airflow", 
}

spark_config = {
    'master':'spark://spark:7077'
}

dag = DAG(
    dag_id="load_dimension_once", 
    start_date=datetime(2016, 2, 1),
    default_args=default_args, 
    schedule_interval="@once"
) 

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

upload_local_to_s3_demographic = SparkSubmitOperator(
    application="/opt/workspace/spark-demographic.py", 
    task_id="local_to_s3_demographic",
    verbose=1,
    conn_id='spark_default',
    name="submit_job",
    conf=spark_config,
    executor_cores=1,
    executor_memory="2G",
    jars='local:///opt/workspace/jars/aws-java-sdk-bundle-1.11.271.jar,local:///opt/workspace/jars/spark-sas7bdat-3.0.0-s_2.12.jar,local:///opt/workspace/jars/parso-2.0.11.jar,local:///opt/workspace/jars/hadoop-aws-3.1.2.jar'
)

upload_local_to_s3_port = SparkSubmitOperator(
    application="/opt/workspace/spark-port.py", 
    task_id="local_to_s3_port",
    verbose=1,
    conn_id='spark_default',
    name="submit_job",
    conf=spark_config,
    executor_cores=1,
    executor_memory="2G",
    jars='local:///opt/workspace/jars/aws-java-sdk-bundle-1.11.271.jar,local:///opt/workspace/jars/spark-sas7bdat-3.0.0-s_2.12.jar,local:///opt/workspace/jars/parso-2.0.11.jar,local:///opt/workspace/jars/hadoop-aws-3.1.2.jar'
)

upload_local_to_s3_addr = SparkSubmitOperator(
    application="/opt/workspace/spark-addr.py", 
    task_id="local_to_s3_addr",
    verbose=1,
    conn_id='spark_default',
    name="submit_job_local_to_s3_addr",
    conf=spark_config,
    executor_cores=1,
    executor_memory="2G",
    jars='local:///opt/workspace/jars/aws-java-sdk-bundle-1.11.271.jar,local:///opt/workspace/jars/spark-sas7bdat-3.0.0-s_2.12.jar,local:///opt/workspace/jars/parso-2.0.11.jar,local:///opt/workspace/jars/hadoop-aws-3.1.2.jar'
)

upload_local_to_s3_cit_res = SparkSubmitOperator(
    application="/opt/workspace/spark-city.py", 
    task_id="local_to_s3_cit_res",
    verbose=1,
    conn_id='spark_default',
    name="submit_job",
    conf=spark_config,
    executor_cores=1,
    executor_memory="2G",
    jars='local:///opt/workspace/jars/aws-java-sdk-bundle-1.11.271.jar,local:///opt/workspace/jars/spark-sas7bdat-3.0.0-s_2.12.jar,local:///opt/workspace/jars/parso-2.0.11.jar,local:///opt/workspace/jars/hadoop-aws-3.1.2.jar'
)

upload_local_to_s3_mode = SparkSubmitOperator(
    application="/opt/workspace/spark-mode.py", 
    task_id="local_to_s3_mode",
    verbose=1,
    conn_id='spark_default',
    name="submit_job_local_to_s3_mode",
    conf=spark_config,
    executor_cores=1,
    executor_memory="2G",
    jars='local:///opt/workspace/jars/aws-java-sdk-bundle-1.11.271.jar,local:///opt/workspace/jars/spark-sas7bdat-3.0.0-s_2.12.jar,local:///opt/workspace/jars/parso-2.0.11.jar,local:///opt/workspace/jars/hadoop-aws-3.1.2.jar'
)

upload_local_to_s3_visa = SparkSubmitOperator(
    application="/opt/workspace/spark-visa.py", 
    task_id="local_to_s3_visa",
    verbose=1,
    conn_id='spark_default',
    name="submit_job",
    conf=spark_config,
    executor_cores=1,
    executor_memory="2G",
    jars='local:///opt/workspace/jars/aws-java-sdk-bundle-1.11.271.jar,local:///opt/workspace/jars/spark-sas7bdat-3.0.0-s_2.12.jar,local:///opt/workspace/jars/parso-2.0.11.jar,local:///opt/workspace/jars/hadoop-aws-3.1.2.jar'
)

stage_demographic_to_redshift = StageToRedshiftOperator(
    task_id='staging_demographic',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    client_type_id='redshift',
    table='staging_demographics',
    s3_bucket='ohmohmprde',
    s3_key="df_demo",
)

stage_port_to_redshift = StageToRedshiftOperator(
    task_id='staging_i94port',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    client_type_id='redshift',
    table='staging_i94port',
    s3_bucket='ohmohmprde',
    s3_key="df_i94port",
)

stage_addr_to_redshift = StageToRedshiftOperator(
    task_id='staging_i94addr',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    client_type_id='redshift',
    table='staging_i94addr',
    s3_bucket='ohmohmprde',
    s3_key="df_i94addr",
)

stage_visa_to_redshift = StageToRedshiftOperator(
    task_id='staging_i94visa',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    client_type_id='redshift',
    table='staging_i94visa',
    s3_bucket='ohmohmprde',
    s3_key="df_i94visa",
)

stage_cit_res_to_redshift = StageToRedshiftOperator(
    task_id='staging_i94cit_res',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    client_type_id='redshift',
    table='staging_i94cit_res',
    s3_bucket='ohmohmprde',
    s3_key="df_i94cit_res",
)

stage_mode_to_redshift = StageToRedshiftOperator(
    task_id='staging_i94mode',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    client_type_id='redshift',
    table='staging_i94mode',
    s3_bucket='ohmohmprde',
    s3_key="df_i94mode",
)

load_demographics_dimension_table = LoadDimensionOperator(
    task_id='Load_demographics_dim_table',
    dag=dag,
    sql=SqlQueries.demographics_table_insert,
    target_table='dim_demographics',
    target_columns='''  state_id, 
                        total_population,
                        male_population,
                        female_population,
                        number_of_veterans,
                        foreign_born,
                        american_indian_and_alaska_native,
                        asian,
                        black_or_african_american,
                        hispanic_or_latino,
                        white   ''',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    client_type_id='redshift',
    append_data=False
)

load_port_dimension_table = LoadDimensionOperator(
    task_id='Load_port_dim_table',
    dag=dag,
    sql=SqlQueries.port_table_insert,
    target_table='dim_i94port',
    target_columns='port_code, port',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    client_type_id='redshift',
    append_data=False
)

load_addr_dimension_table = LoadDimensionOperator(
    task_id='Load_addr_dim_table',
    dag=dag,
    sql=SqlQueries.addr_table_insert,
    target_table='dim_i94addr',
    target_columns='state_id, fullname_state',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    client_type_id='redshift',
    append_data=False
)

load_visa_dimension_table = LoadDimensionOperator(
    task_id='Load_visa_dim_table',
    dag=dag,
    sql=SqlQueries.visa_table_insert,
    target_table='dim_i94visa',
    target_columns='visa_id, reason',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    client_type_id='redshift',
    append_data=False
)

load_cit_res_dimension_table = LoadDimensionOperator(
    task_id='Load_cit_res_dim_table',
    dag=dag,
    sql=SqlQueries.cit_res_table_insert,
    target_table='dim_i94cit_res',
    target_columns='city_id, city',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    client_type_id='redshift',
    append_data=False
)

load_mode_dimension_table = LoadDimensionOperator(
    task_id='Load_mode_dim_table',
    dag=dag,
    sql=SqlQueries.mode_table_insert,
    target_table='dim_i94mode',
    target_columns='transport_code, transport_type',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    client_type_id='redshift',
    append_data=False
)


run_quality_checks_demographics = DataQualityOperator(
    task_id='Run_data_quality_checks_demographics',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.demographics_fact_table_check_null,
    result=0,
)

run_quality_checks_port = DataQualityOperator(
    task_id='Run_data_quality_checks_port',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.port_fact_table_check_null,
    result=0,
)

run_quality_checks_addr = DataQualityOperator(
    task_id='Run_data_quality_checks_addr',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.addr_fact_table_check_null,
    result=0,
)

run_quality_checks_visa = DataQualityOperator(
    task_id='Run_data_quality_checks_visa',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.visa_fact_table_check_null,
    result=0,
)

run_quality_checks_city = DataQualityOperator(
    task_id='Run_data_quality_checks_city',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.city_fact_table_check_null,
    result=0,
)

run_quality_checks_mode = DataQualityOperator(
    task_id='Run_data_quality_checks_mode',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.mode_fact_table_check_null,
    result=0,
)

checks_port_duplicated = DataQualityCheckDuplicateOperator(
    task_id='checks_port_duplicated',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.checks_port_duplicated,
    result="",
)

checks_visa_duplicated = DataQualityCheckDuplicateOperator(
    task_id='checks_visa_duplicated',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.checks_visa_duplicated,
    result="",
)

checks_addr_duplicated = DataQualityCheckDuplicateOperator(
    task_id='checks_addr_duplicated',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.checks_addr_duplicated,
    result="",
)

checks_i94cit_res_duplicated = DataQualityCheckDuplicateOperator(
    task_id='checks_i94cit_res_duplicated',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.checks_i94cit_res_duplicated,
    result="",
)

checks_mode_duplicated = DataQualityCheckDuplicateOperator(
    task_id='checks_mode_duplicated',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.checks_mode_duplicated,
    result="",
)

checks_demographics_duplicated = DataQualityCheckDuplicateOperator(
    task_id='checks_demographics_duplicated',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.checks_demographics_duplicated,
    result="",
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> upload_local_to_s3_demographic
start_operator >> upload_local_to_s3_port
start_operator >> upload_local_to_s3_addr
start_operator >> upload_local_to_s3_visa
start_operator >> upload_local_to_s3_cit_res
start_operator >> upload_local_to_s3_mode

upload_local_to_s3_demographic >> stage_demographic_to_redshift
upload_local_to_s3_port >> stage_port_to_redshift
upload_local_to_s3_addr >> stage_addr_to_redshift
upload_local_to_s3_visa >> stage_visa_to_redshift
upload_local_to_s3_cit_res >> stage_cit_res_to_redshift
upload_local_to_s3_mode >> stage_mode_to_redshift


stage_demographic_to_redshift >> load_demographics_dimension_table
stage_port_to_redshift >> load_port_dimension_table
stage_addr_to_redshift >> load_addr_dimension_table
stage_visa_to_redshift >> load_visa_dimension_table
stage_cit_res_to_redshift>> load_cit_res_dimension_table
stage_mode_to_redshift>> load_mode_dimension_table

load_demographics_dimension_table >> run_quality_checks_demographics
load_port_dimension_table >> run_quality_checks_port
load_addr_dimension_table >> run_quality_checks_addr
load_visa_dimension_table >> run_quality_checks_visa
load_cit_res_dimension_table >> run_quality_checks_city
load_mode_dimension_table >> run_quality_checks_mode

run_quality_checks_demographics >> end_operator
run_quality_checks_port >> end_operator
run_quality_checks_addr >> end_operator
run_quality_checks_visa >> end_operator
run_quality_checks_city >> end_operator
run_quality_checks_mode >> end_operator

load_demographics_dimension_table >> checks_demographics_duplicated
load_port_dimension_table >> checks_port_duplicated
load_addr_dimension_table >> checks_addr_duplicated
load_visa_dimension_table >> checks_visa_duplicated
load_cit_res_dimension_table >> checks_i94cit_res_duplicated
load_mode_dimension_table >> checks_mode_duplicated

checks_demographics_duplicated >> end_operator
checks_i94cit_res_duplicated >> end_operator
checks_addr_duplicated >> end_operator
checks_visa_duplicated >> end_operator
checks_port_duplicated >> end_operator
checks_mode_duplicated >> end_operator