from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """Operator responsible to transfer data in S3 to Redshift"""
    ui_color = '#358140'
    template_fields = ('s3_bucket', 's3_key', 'table', 'table_queries')

    @apply_defaults
    def __init__(self,
                s3_bucket: str,
                s3_key: str,
                schema: str,
                table: str,
                redshift_conn_id: str,
                aws_credentials_id: str,
                region: str,
                table_queries: dict,
                *args, **kwargs) -> None:

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.schema = schema
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.table_queries = table_queries


    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'

        s3_hook = S3Hook(aws_conn_id=self.aws_credentials_id)
        credentials = s3_hook.get_credentials()

        create_query = self.table_queries.get("create").format(schema=self.schema, table=self.table)
        truncate_query = f"TRUNCATE {self.schema}.{self.table};"
        copy_query = self.table_queries.get("copy").format(schema=self.schema, table=self.table, s3_path=s3_path,
            region=self.region, aws_key=credentials.access_key, aws_secret=credentials.secret_key)

        formated_query = f"""
            BEGIN;
            {create_query}
            {truncate_query}
            {copy_query}
            COMMIT
        """
        postgres_hook.run(formated_query, True)
        