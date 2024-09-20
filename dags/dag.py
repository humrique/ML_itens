from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6 ,20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(dag_id='fetch_and_store_mercado_livre_itens',
        default_args=default_args,
        description='DAG que itens do Mercado Livre e armazena no Postgres',
        schedule_interval=timedelta(days=1)):

    start = EmptyOperator(task_id='start')

    @task
    def task_1():
        return ''

    end = EmptyOperator(task_id='end')

    start >> task_1() >> end



