from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from tasks.data_collection import get_ml_data_itens
from tasks.postgres_insert import insert_item_data_into_postgres

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='fetch_and_store_mercado_livre_itens',
    default_args=default_args,
    description='DAG que coleta itens do Mercado Livre e armazena no Postgres',
    schedule_interval=timedelta(days=1),
)

start = EmptyOperator(task_id='start', dag=dag)

fetch_item_data_task = PythonOperator(
    task_id='fetch_item_data',
    python_callable=get_ml_data_itens,
    op_args=['bicicleta', 50],
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='ml_itens_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS itens (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        price TEXT,
        rating TEXT,
        store TEXT
    );
    """,
    dag=dag,
)

insert_item_data_task = PythonOperator(
    task_id='insert_item_data',
    python_callable=insert_item_data_into_postgres,
    dag=dag,
)

end = EmptyOperator(task_id='end', dag=dag)

start >> fetch_item_data_task >> create_table_task >> insert_item_data_task >> end
