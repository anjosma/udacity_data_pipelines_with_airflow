from typing import List

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
    ) -> DAG:
    """Creates tasks to create Dimension Tables listed in `dimension_tables`

    Args:
        parent_dag (str): Parent Dag Name
        child_dag (str): Child Dag Name
        redshift_conn_id (str): Redshift connection ID created in Airflow Connections
        schema (str): Name of schema in database 
        dimension_tables (List[str]): List of name of tables to create the dimension tables
        config_tables (dict): Dict containing create and insert queries related to given table
        default_args (dict): Dict containing default arguments of Parent Dag

    Returns:
        DAG: Airflow DAG Object with tasks created in Child Dag
    """

    dag = DAG(
        dag_id=f"{parent_dag}.{child_dag}",
        default_args=default_args, **kwargs)

    for table in dimension_tables:
        create_query = config_tables.get(table).get('create')
        insert_query = config_tables.get(table).get('insert')

        load_dimension = LoadDimensionOperator(
            task_id=f"load_{table}_dim_table",
            redshift_conn_id=redshift_conn_id,
            schema=schema,
            table=table,
            create_query=create_query,
            insert_query=insert_query,
            dag=dag
        )

    return dag