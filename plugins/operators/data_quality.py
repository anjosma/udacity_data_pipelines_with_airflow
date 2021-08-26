from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """Operator responsible to check quality of created tables"""

    ui_color = '#89DA59'
    template_fields = ('schema', 'table', 'test_queries')

    @apply_defaults
    def __init__(self,
                schema: str,
                table: str,
                redshift_conn_id: str,
                test_queries: list,
                *args, 
                **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.schema = schema
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.test_queries = test_queries

    def __check_quality(self, rule, check_query):
        self.log.info(check_query)
        records = self.postgres_hook.get_records(check_query.get('query'))
        n_records = records[0][0]
        if n_records != check_query.get('expected_result'):
            raise ValueError(f"Error: Table {self.table} returns not expected result from rule {rule}")
        self.log.info(f"Passed: Table {self.table} contains {n_records} records")

    def execute(self, context):
        self.postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for quality_test in self.test_queries:
            rule = list(quality_test.keys())[0]
            self.__check_quality(rule, quality_test.get(rule))
        [self.__check_quality(quality_test.get()) for quality_test in self.test_queries]
