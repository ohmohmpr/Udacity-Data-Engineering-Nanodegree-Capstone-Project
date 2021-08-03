from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityCheckDuplicateOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 result="",
                 *args, **kwargs):

        super(DataQualityCheckDuplicateOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql=sql
        self.result=result
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        records = redshift.get_records(self.sql)
        if  records == self.result:
            raise ValueError(f"Data quality check duplicate failed. The table returned duplicated results")
            
        logging.info(f"There is no duplicated row in this table")

