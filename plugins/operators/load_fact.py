from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    template_fields = ('database', 'schema', 'table', 'redshift_conn_id', 'region', 'insert_query', 'create_query')

    @apply_defaults
    def __init__(self,
                database,
                schema,
                table,
                redshift_conn_id,
                region,
                insert_query,
                create_query,
                *args,
                **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.database = database
        self.schema = schema
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.region = region
        self.insert_query = insert_query
        self.create_query = create_query

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        create_query = self.create_query.format(schema=self.schema, table=self.table)
        truncate_query = f"TRUNCATE {self.schema}.{self.table};"
        insert = self.insert_query.format(schema=self.schema, table=self.table)

        formated_query = f"""
            BEGIN;
            {create_query}
            {truncate_query}
            {insert}
            COMMIT
        """
        postgres_hook.run(formated_query, True)
