# import librairies

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# define DAG parameters

default_args = {
    'owner': "Kimba SABI N'GOYE",
    'start_date': days_ago(0),
    'email': ['kimbasabingoye@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}