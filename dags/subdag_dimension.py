from typing import List

import datetime

from airflow import DAG
from operators import (LoadDimensionOperator)

def load_dimension_subdag(
    parent_dag: str, 
    child_dag: str,
    redshift_conn_id: str,
    schema: str,
    dimension_tables: List[str],
    config_tables: dict,
    default_args: dict,
    *args,
    **kwargs
    ):

    dag = DAG(
        dag_id=f"{parent_dag}.{child_dag}",
        default_args=default_args, **kwargs)

    for table in dimension_tables:
        create_query = config_tables.get(table).get('create')
        insert_query = config_tables.get(table).get('insert')

        loads = []
        load_dimension = LoadDimensionOperator(
            task_id=f"load_{table}_dim_table",
            redshift_conn_id=redshift_conn_id,
            schema=schema,
            table=table,
            create_query=create_query,
            insert_query=insert_query,
            dag=dag
        )
        loads.append(load_dimension)

    return dag