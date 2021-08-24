from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    template_fields = ('schema', 'table')

    @apply_defaults
    def __init__(self,
                schema,
                table,
                redshift_conn_id,
                *args, 
                **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.schema = schema
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.check_query = "SELECT COUNT(*) FROM {schema}.{table}"

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        check_query = self.check_query.format(schema=self.schema, table=self.table)

        records = postgres_hook.get_records(check_query)

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")