from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime, timedelta
from scripts.functions import download_datasets, process_datasets

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes= 5)
}

with DAG(
        "data-ops_etl",
        start_date= datetime(2021,1,1),
        schedule_interval= "@daily",
        default_args= default_args, 
        catchup= False) as dag :

    # Extract
    download_files = PythonOperator(
        task_id="download_files",
        python_callable=download_datasets)

    are_title_basics_available = FileSensor(
        task_id="are_title_basics_available",
        filepath="/home/airflow/dags/files/title.basics.tsv.gz",
        poke_interval=5,
        timeout=10
    )

    are_title_ratings_available = FileSensor(
        task_id="are_title_ratings_available",
        filepath="/home/airflow/dags/files/title.ratings.tsv.gz",
        poke_interval=5,
        timeout=10
    )

    are_title_crew_available = FileSensor(
        task_id="are_title_crew_available",
        filepath="/home/airflow/dags/files/title.crew.tsv.gz",
        poke_interval=5,
        timeout=10
    )

    # Transform & Load
    process_data = PythonOperator(
        task_id="process_data",
        python_callable=process_datasets)

    # Order of execution
    download_files >> [are_title_basics_available, are_title_ratings_available, are_title_crew_available] >> process_data