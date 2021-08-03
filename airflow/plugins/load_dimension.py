from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import logging


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 client_type_id="",
                 aws_credentials_id="",
                 sql="",
                 target_table="",
                 target_columns="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.client_type_id=client_type_id
        self.aws_credentials_id=aws_credentials_id
        self.sql=sql
        self.target_table=target_table
        self.target_columns=target_columns
        self.append_data=append_data

    def execute(self, context):
        aws_hook = AwsHook(aws_conn_id=self.aws_credentials_id,client_type=self.client_type_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        if self.append_data == True:
            formatted_sql = self.sql.format(
                self.target_table,
                self.target_columns,
            )
            redshift.run(formatted_sql)
            logging.info("load dimension(append) successfully.")
        else:
            logging.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.target_table))
            
            formatted_sql = self.sql.format(
                self.target_table,
                self.target_columns,
            )
            redshift.run(formatted_sql)
            logging.info("load dimension(truncate) successfully.")
    


