from __future__ import annotations

import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dag_datamart_nida",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2023, 5, 21, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),

) as dag:
    delete_data = BashOperator(
        task_id="delete_datamart",
        bash_command="python3 /root/coba2/nida/datamart_delete_nida.py ",
    )

    insert_data = BashOperator(
        task_id="insert_datamart",
        bash_command="python3 /root/coba2/nida/datamart_insert_nida.py ",
    )

    delete_data >> insert_data

if __name__ == "__main__":
    dag.test()