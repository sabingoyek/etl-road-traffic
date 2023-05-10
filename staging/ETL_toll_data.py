# import librairies

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Task 1.1 - Define DAG arguments

default_args = {
    'owner': "Kimba SABI N'GOYE",
    'start_date': days_ago(0),
    'email': ['******'],
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
    task_id="unzip-data",
    bash_command="tar -xvzf /home/sabingoyek/airflow/dags/tolldata.tgz -C /home/sabingoyek/airflow/dags/",
    dag=dag
)

# Task 1.4 - Create a task to extract data from csv file

extract_data_from_csv = BashOperator(
    task_id="extract-data-from-csv",
    bash_command="cut -d',' -f1-4 /home/sabingoyek/airflow/dags/vehicle-data.csv > /home/sabingoyek/airflow/dags/csv_data.csv",
    dag=dag
)

# Task 1.5 - Create a task to extract data from tsv file

extract_data_from_tsv = BashOperator(
    task_id="extract-data-from-tsv",
    bash_command="cut -d$'\t' -f5-7 /home/sabingoyek/airflow/dags/tollplaza-data.tsv | tr '\t' ',' > /home/sabingoyek/airflow/dags/tsv_data.csv",
    dag=dag
)

# Task 1.6 - Create a task to extract data from fixed width file
# extract last two field

extract_data_from_fixed_width = BashOperator(
    task_id="extract-data-fixed_width",
    bash_command="cat /home/sabingoyek/airflow/dags/payment-data.txt | tr -s '[:space:]' | rev | cut -d' ' -f1,2 | rev | tr ' ' ',' > /home/sabingoyek/airflow/dags/fixed_width_data.csv",
    dag=dag
)

# Task 1.7 - Create a task to consolidate data extracted from previous tasks

consolidate_data = BashOperator(
    task_id="consolidate-data",
    bash_command="paste -d',' /home/sabingoyek/airflow/dags/csv_data.csv /home/sabingoyek/airflow/dags/tsv_data.csv /home/sabingoyek/airflow/dags/fixed_width_data.csv > /home/sabingoyek/airflow/dags/extracted_data.csv",
    dag=dag
)

# Task 1.8 - Transform and load the data

transform_data = BashOperator(
    task_id="transform-data",
    bash_command="paste -d',' <(cut -d',' -f1-3 /home/sabingoyek/airflow/dags/extracted_data.csv)  <(cut -d',' -f4 /home/sabingoyek/airflow/dags/extracted_data.csv | tr '[:lower:]' '[:upper:]') <(cut -d',' -f5- /home/sabingoyek/airflow/dags/extracted_data.csv) > /home/sabingoyek/airflow/dags/transformed_data.csv",
    dag=dag
)

# Task 1.9 - Define the task pipeline

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data