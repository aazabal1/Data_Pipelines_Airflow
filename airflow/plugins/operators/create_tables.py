from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class PostgresOperator(BaseOperator):
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 postgres_conn_id="",
                 sql="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        
    def execute(self, context):
        self.log.info('PostgresOperator implemented')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Creating tables")
        redshift.run(self.sql)
