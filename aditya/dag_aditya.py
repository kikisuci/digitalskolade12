from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

# Define the DAG
dag = DAG(
    dag_id='dag_aditya',
    schedule_interval='3 2 * * *',
    start_date=pendulum.datetime(2023, 5, 23, tz="Asia/Jakarta"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

# Task 1: Data Ingestion
data_ingestion_task = BashOperator(
    task_id='data_ingestion',
    bash_command='python3 /root/airflow/dags/aditya/data-ingestion/main.py',
    dag=dag
)

sf_task = BashOperator(
    task_id="snowflake_warehouse",
    bash_command="echo 1",
    dag=dag
)

data_ingestion_task >> sf_task

# Task 2: Daily Gross Revenue
daily_gross_revenue_task = BashOperator(
    task_id='daily_gross_revenue',
    bash_command='python3 /root/airflow/dags/aditya/sf-data-mart/daily_gross_revenue.py',
    dag=dag
)
sf_task >> daily_gross_revenue_task

# Task 3: Monthly Category Sales
monthly_category_sales_task = BashOperator(
    task_id='monthly_category_sales',
    bash_command='python3 /root/airflow/dags/aditya/sf-data-mart/monthly_category_sales.py',
    dag=dag
)
sf_task >> monthly_category_sales_task

# Task 4: Monthly Country Sales
monthly_country_sales_task = BashOperator(
    task_id='monthly_country_sales',
    bash_command='python /root/airflow/dags/aditya/sf-data-mart/monthly_country_sales.py',
    dag=dag
)
sf_task >> monthly_country_sales_task

# Task 5: Monthly Gross Revenue
monthly_gross_revenue_task = BashOperator(
    task_id='monthly_gross_revenue',
    bash_command='python3 /root/airflow/dags/aditya/sf-data-mart/monthly_gross_revenue.py',
    dag=dag
)
sf_task >> monthly_gross_revenue_task

# Task 6: Monthly Product Sales
monthly_product_sales_task = BashOperator(
    task_id='monthly_product_sales',
    bash_command='python3 /root/airflow/dags/aditya/sf-data-mart/monthly_product_sales.py',
    dag=dag
)
sf_task >> monthly_product_sales_task

