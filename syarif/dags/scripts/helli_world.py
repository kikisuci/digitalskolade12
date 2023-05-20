from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_helli():
    return 'Helli world from first Airflow DAG!'

dag = DAG('helli_world', description='Helli World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

helli_operator = PythonOperator(task_id='helli_task', python_callable=print_helli, dag=dag)

helli_operator