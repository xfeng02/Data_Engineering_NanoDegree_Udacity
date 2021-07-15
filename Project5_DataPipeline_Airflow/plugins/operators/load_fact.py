from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql_stmt="",
            
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.sql_stmt=sql_stmt
        
    
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
              
        self.log.info(f"Inserting data to fact table--{self.table}")
        formatted_sql= f"Insert into {self.table} {self.sql_stmt}" 
        self.log.info("print insert statement: " + formatted_sql)
        redshift.run(formatted_sql)
        self.log.info(f"Done inserting into {self.table}")
        