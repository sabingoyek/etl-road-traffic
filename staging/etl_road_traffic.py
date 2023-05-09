# import librairies

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Task 1.1 - Define DAG arguments

default_args = {
    'owner': "Kimba SABI N'GOYE",
    'start_date': days_ago(0),
    'email': ['kimbasabingoye@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Task 1.2 - Define the DAG

dag = DAG(
    dag_id="ETL_toll_data",
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description="Apache Airflow Final Assignment"
)

# Task 1.3 - Create a task to unzip data

unzip_data = BashOperator(
    task_id="unzip data",
    bash_command="tar -xvzf /home/sabingoyek/airflow/dags/etl-road-traffic/tolldata.tgz -C /home/sabingoyek/airflow/dags/etl-road-traffic/",
    dag=dag
)

