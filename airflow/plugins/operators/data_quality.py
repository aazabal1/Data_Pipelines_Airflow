from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    check_null_sql = """
    SELECT count({})
    FROM {}
    WHERE {} IS NULL
    """
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="",
                 table = "",
                 column_check = "",
                 expected_result = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.column_check = column_check
        self.expected_result = expected_result
        
    def execute(self, context):
        self.log.info('DataQualityOperator implemented')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Checking null values in {self.column_check} column from {self.table}")
        formatted_sql = DataQualityOperator.check_null_sql.format(
                        self.column_check,
                        self.table,
                        self.column_check
        )
        check_val = redshift.get_records(formatted_sql)[0]
        self.log.info(f'Printing check results: {check_val[0]}')
        
        if check_val[0] != self.expected_result:
            self.log.info('Tests failed')
            raise ValueError('Data quality check failed')

