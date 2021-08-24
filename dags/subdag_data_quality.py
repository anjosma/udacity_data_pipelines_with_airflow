from typing import List

from airflow import DAG
from operators import (DataQualityOperator)

def check_tables_subdag(
    parent_dag: str, 
    child_dag: str,
    redshift_conn_id: str,
    schema: str,
    check_tables: List[str],
    default_args: dict,
    *args,
    **kwargs
    ):

    dag = DAG(
        dag_id=f"{parent_dag}.{child_dag}",
        default_args=default_args, **kwargs)

    for table in check_tables:

        check_table = DataQualityOperator(
            task_id=f"check_{table}_table",
            redshift_conn_id=redshift_conn_id,
            schema=schema,
            table=table,
            dag=dag
        )

    return dag