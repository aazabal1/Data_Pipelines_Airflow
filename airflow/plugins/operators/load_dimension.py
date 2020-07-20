from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 append_data=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.append_data = append_data
        
    def execute(self, context):
        self.log.info('LoadDimensionOperator implemented')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Loading data into dimension table in Redshift")
        if self.append_data==True:
            table_insert_sql = f"""
                INSERT INTO {self.table}
                {self.select_sql}
            """
        else:
            table_insert_sql = f"""
                DELETE FROM {self.table}
                INSERT INTO {self.table}
                {self.select_sql}
            """
        redshift.run(table_insert_sql)
