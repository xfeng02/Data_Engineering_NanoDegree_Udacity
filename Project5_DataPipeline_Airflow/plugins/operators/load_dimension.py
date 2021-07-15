from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql_stmt="",
                 mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
      
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.sql_stmt=sql_stmt
        self.mode=mode
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.mode=="delete-load":
            self.log.info(f"Clearing data from dimension table--{self.table}")
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info(f"Inserting data to dimension table--{self.table}")
        formatted_sql= f"Insert into {self.table} {self.sql_stmt}" 
        self.log.info("print insert statement: " + formatted_sql)
        redshift.run(formatted_sql)
        self.log.info(f"Done inserting into {self.table}")
