from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_hella():
    return 'Hella world from first Airflow DAG!'

dag = DAG('hella_world', description='Hella World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hella_operator = PythonOperator(task_id='hella_task', python_callable=print_hella, dag=dag)

hella_operator