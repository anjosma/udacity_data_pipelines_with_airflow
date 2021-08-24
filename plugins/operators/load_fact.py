from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """Operator responsible for creates Fact table in DataWarehouse"""

    ui_color = '#F98866'
    template_fields = ('schema', 'table', 'redshift_conn_id', 'insert_query', 'create_query')

    @apply_defaults
    def __init__(self,
                schema,
                table,
                redshift_conn_id,
                insert_query,
                create_query,
                truncate: bool = True,
                *args,
                **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.schema = schema
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.insert_query = insert_query
        self.create_query = create_query
        self.truncate = truncate

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        create_query = self.create_query.format(schema=self.schema, table=self.table)
        truncate_query = f"TRUNCATE {self.schema}.{self.table};"
        insert = self.insert_query.format(schema=self.schema, table=self.table)

        formated_query = f"""
            BEGIN;
            {create_query if self.create_query else None}
            {truncate_query if self.truncate else None}
            {insert}
            COMMIT
        """
        postgres_hook.run(formated_query, True)
