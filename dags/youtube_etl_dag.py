from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from etl.extract import run_extract
from etl.transform import run_transform
from etl.load import run_load

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='youtube_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['youtube', 'etl'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=run_extract,
        params={
            'query': 'kpop,music,hiphop',
            'max_total': 50,
            'api_key': '{{ var.value.YT_API_KEY }}'  # Airflow Variable로 관리
        }
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=run_transform,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=run_load,
        params={
            'db_url': '{{ var.value.YT_DB_URL }}',
            'upstream_task_id': 'transform_data'
        },
        provide_context=True,
    )

    extract_task >> transform_task >> load_task
