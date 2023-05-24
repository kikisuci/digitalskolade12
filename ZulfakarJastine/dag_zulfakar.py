from __future__ import annotations
import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dag_zulfakar",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 5, 23, tz="Asia/Jakarta"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),

) as dag:
    destroy_data = BashOperator(
        task_id="drop_datamart",
        bash_command="python3 /root/airflow/dags/ZulfakarJastine/drop_datamart.py",
    )

    insert_data = BashOperator(
        task_id="insert_datamart",
        bash_command="python3 /root/airflow/dags/ZulfakarJastine/insert_datamart.py",
    )

    destroy_data >> insert_data

if __name__ == "__main__":
    dag.test()