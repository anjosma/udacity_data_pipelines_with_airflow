from typing import List

from airflow import DAG
from operators import (DataQualityOperator)

def check_tables_subdag(
    parent_dag: str, 
    child_dag: str,
    redshift_conn_id: str,
    schema: str,
    config_quality: dict,
    default_args: dict,
    *args,
    **kwargs
    ) -> DAG:
    """Creates tasks to check Data Quality in listed table passed in `check_tables`

    Args:
        parent_dag (str): Parent Dag Name
        child_dag (str): Child Dag Name
        redshift_conn_id (str): Redshift connection ID created in Airflow Connections
        schema (str): Name of schema in database 
        check_tables (List[str]): List of tables to check in `DataQualityOperator`
        default_args (dict): Dict containing default arguments of Parent Dag

    Returns:
        DAG: Airflow DAG Object with tasks created in Child Dag
    """

    dag = DAG(
        dag_id=f"{parent_dag}.{child_dag}",
        default_args=default_args, **kwargs)

    for table in config_quality:

        check_table = DataQualityOperator(
            task_id=f"check_{table}_table",
            redshift_conn_id=redshift_conn_id,
            schema=schema,
            test_queries=config_quality.get(table),
            table=table,
            dag=dag
        )

    return dag