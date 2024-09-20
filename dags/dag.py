from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
from bs4 import BeautifulSoup

def return_item_value_text_strip(value, default_return):
    return value.text.strip() if value else default_return


def get_ml_data_itens(item, num_itens, ti):

    #Headers para a requisição no ML
    headers = {
        "Referer": 'https://lista.mercadolivre.com.br/',
        "Sec-Ch-Ua": "Not_A Brand",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "Windows",
        "User-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    }

    # Item e Quantidade de Itens para a Busca
    item = "bicicleta"
    num_itens = 20
    # URL base para a consulta no Mercado Livre
    base_url = f"https://lista.mercadolivre.com.br/{item}"


    # Lista para armazenar os itens
    itens = []

    # Loop while para obtermos o número necessário de itens
    while len(itens) < num_itens:
        url = f"{base_url}&_Desde_{len(itens)}_NoIndex_True"   
        
        # Requisição para a URL
        response = requests.get(url, headers=headers)
        
        # Verifica se a requisição foi bem-sucedida
        if response.status_code == 200:
            
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Procura pelos itens da lista, baseado na classe específica do HTML do Mercado Livre
            item_containers = soup.find_all("li", {"class": "ui-search-layout__item"})
            
            # Loop através dos itens encontrados na página
            for itemMl in item_containers:
                title = itemMl.find("a", {"class": "ui-search-link__title-card"})            
                price_inter = itemMl.find("span", {"class": "andes-money-amount__fraction"}).text.strip() 
                price_cents = itemMl.find("span", {"class": "andes-money-amount__cents"})            
                rating = itemMl.find("span", {"class": "ui-search-reviews__rating-number"})
                store = itemMl.find("p", {"class": "ui-search-official-store-label"})
                
                if title:
                    itemMl_title = title.text.strip()
                    price_cents_value = return_item_value_text_strip(price_cents, "0")
                    rating_value = return_item_value_text_strip(rating, "Sem Informação")
                    store_value = return_item_value_text_strip(store, "Não informado")          
                            
                    # Adiciona o item à lista de itens coletados    
                    itens.append({
                        "Title": itemMl_title,                        
                        "Price": price_inter + "." + price_cents_value,
                        "Rating": rating_value,
                        "Store": store_value,
                    })
                    # Adiciona o item à lista de itens coletados
                    print(itemMl_title + " " + store_value)       
            
        else:
            print("Failed to retrieve the page")
            break

    # Limita o número de itens coletados ao valor definido por 'num_itens'
    itens = itens[:num_itens]

    # Converte itens em um DataFrame
    df = pd.DataFrame(itens)

    # Remove duplicatas no DataFrame, baseando-se nas colunas 'Title' e 'Price'# Remove duplicates based on 'Title' column
    df.drop_duplicates(subset=["Title","Price"], inplace=True)

    ti.xcom_push(key='item_data', value=df.to_dict('records'))

def insert_book_data_into_postgres(ti):
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


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='fetch_and_store_mercado_livre_itens',
        default_args=default_args,
        description='DAG que itens do Mercado Livre e armazena no Postgres',
        schedule_interval=timedelta(days=1),)

start = EmptyOperator(task_id='start')

fetch_item_data_task = PythonOperator(
    task_id='fetch_item_data',
    python_callable=get_ml_data_itens,
    op_args=['bicileta',50],  # Number of books to fetch
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
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)


end = EmptyOperator(task_id='end')

start >> fetch_item_data_task >> create_table_task >> insert_item_data_task >> end



