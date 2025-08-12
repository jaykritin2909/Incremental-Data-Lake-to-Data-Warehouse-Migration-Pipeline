from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['alerts@datateam.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    's3_to_redshift_migration',
    default_args=default_args,
    description='Incremental Data Lake to Data Warehouse Migration',
    schedule_interval='@daily',
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=['data-migration']
) as dag:

    migrate_data = AwsGlueJobOperator(
        task_id="run_glue_migration",
        job_name="data_lake_to_redshift_migration",
        script_args={"--last_run_date": "{{ ds }}"}
    )

    migrate_data
