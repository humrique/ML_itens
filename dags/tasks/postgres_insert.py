from airflow.providers.postgres.hooks.postgres import PostgresHook

def insert_item_data_into_postgres(ti):
    item_data = ti.xcom_pull(key='item_data', task_ids='fetch_item_data')
    if not item_data:
        raise ValueError("No item data found")

    postgres_hook = PostgresHook(postgres_conn_id='ml_itens_connection')
    insert_query = """
    INSERT INTO itens (title, price, rating, store)
    VALUES (%s, %s, %s, %s)
    """
    for item in item_data:
        postgres_hook.run(insert_query, parameters=(item['Title'], item['Price'], item['Rating'], item['Store']))
