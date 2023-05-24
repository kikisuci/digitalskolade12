from __future__ import annotations

import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dag_config_nida",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2023, 5, 20, tz="Asia/Jakarta"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),

) as dag:
    run_generator = BashOperator(
        task_id="update_report_date",
        bash_command="python3 /root/coba2/nida/config_nida.py ",
    )

    run_generator

if __name__ == "__main__":
    dag.test()
