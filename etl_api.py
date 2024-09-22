import httpx
import json
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine
import time
import datetime
from dotenv import load_dotenv
import os


def get_URI(query: str, page_num: str, date: str, API_KEY: str) -> str:
    """Возвращает URL к статьям для текущего запроса по номеру страницы и дате."""

    uri = f'https://api.nytimes.com/svc/search/v2/articlesearch.json?q={query}'
    uri = uri + f'&page={page_num}&begin_date={date}&end_date={date}'
    uri = uri + f'&api-key={API_KEY}'

    return uri

def extract():
    df = pd.DataFrame()

    current_date = datetime.datetime.now().strftime('%Y%m%d')
    page_num = 1

    load_dotenv()
    API_KEY = os.getenv('API_KEY')

    while True:
        # Получаю URI с записями, относящимся к COVID
        URI = get_URI('COVID', page_num=str(page_num), date=current_date, API_KEY=API_KEY)

        response = httpx.get(URI)
        data = response.json()

        df_request = json_normalize(data['response'], record_path=['docs'])

        # Прервать, если новые записи отсутствуют
        if df_request.empty:
            break

        df = pd.concat([df, df_request])

        time.sleep(6)


if __name__ == '__main__':
    extract()
    # transform()
    # load()