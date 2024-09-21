import requests
import pandas as pd
from bs4 import BeautifulSoup
from utils.text_utils import return_item_value_text_strip

def get_ml_data_itens(item, num_itens, ti):
    headers = {
        "Referer": 'https://lista.mercadolivre.com.br/',
        "User-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    }

    base_url = f"https://lista.mercadolivre.com.br/{item}"
    itens = []

    while len(itens) < num_itens:
        url = f"{base_url}&_Desde_{len(itens)}_NoIndex_True"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            item_containers = soup.find_all("li", {"class": "ui-search-layout__item"})
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

                    itens.append({
                        "Title": itemMl_title,
                        "Price": price_inter + "." + price_cents_value,
                        "Rating": rating_value,
                        "Store": store_value,
                    })

    df = pd.DataFrame(itens)
    df.drop_duplicates(subset=["Title", "Price"], inplace=True)
    ti.xcom_push(key='item_data', value=df.to_dict('records'))
