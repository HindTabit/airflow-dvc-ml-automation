from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ml_continuous_training',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # 1. Le Sensor : Attend qu'un fichier 'new_data.csv' apparaisse dans le dossier data
    wait_for_new_data = FileSensor(
        task_id='wait_for_new_data',
        filepath='data/raw_data.csv',
        fs_conn_id='my_file_system', # À configurer dans l'UI Airflow
        poke_interval=10,
        timeout=600
    )

    # 2. Exécution de DVC : Versionne et entraîne
    run_dvc_pipeline = BashOperator(
        task_id='run_dvc_pipeline',
        bash_command='cd /opt/airflow && dvc repro'
    )

    wait_for_new_data >> run_dvc_pipeline