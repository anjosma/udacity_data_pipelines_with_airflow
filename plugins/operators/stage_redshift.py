from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                s3_bucket: str,
                s3_key: str,
                database: str,
                schema: str,
                table: str,
                redshift_conn_id: str,
                aws_credentials_id: str,
                table_queries: dict,
                *args, **kwargs) -> None:

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.database = database
        self.schema = schema
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table_queries = table_queries


    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        query = self.table_queries.get("create").format(schema=self.schema, table=self.table)
        





