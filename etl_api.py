import httpx
import json
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine
import time
import datetime
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv('API_KEY')
DATABASE_URL = os.getenv('DATABASE_URL')

def get_URI(query: str, page_num: str, date: str, API_KEY: str) -> str:
    """Возвращает URL к статьям для текущего запроса по номеру страницы и дате."""

    uri = f'https://api.nytimes.com/svc/search/v2/articlesearch.json?q={query}'
    uri = uri + f'&page={page_num}&begin_date={date}&end_date={date}'
    uri = uri + f'&api-key={API_KEY}'

    return uri

def extract() -> pd.DataFrame:
    """Извлечение данных из API NYTimes."""
    df = pd.DataFrame()

    # current_date = datetime.datetime.now().strftime('%Y%m%d')
    current_date = '20210101'
    page_num = 1

    while True:
        # Получаю URI с записями, относящимся к COVID
        URI = get_URI('COVID', page_num=str(page_num), date=current_date, API_KEY=API_KEY)

        response = httpx.get(URI)
        data = response.json()

        try:
            df_request = json_normalize(data['response'], record_path=['docs'])
        except Exception as e:
            break

        # Прервать, если новые записи отсутствуют
        if df_request.empty:
            break

        df = pd.concat([df, df_request])

        time.sleep(6)

    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Преобразование данных: очистка и фильтрация."""
    # Поиск дубликатов по id статьи и их удаление
    if len(df['_id'].unique()) < len(df):
        print(f"В датасете содержатся дубликаты. Общее количество строк было: {len(df)}")
        df = df.drop_duplicates('_id', keep='first')

    # Поиск и удаление записей без заголовкой
    if df['headline.main'].isnull().any():
        print("В датасете нашлись записи без заголовка.")
        # Фильтр строк, где заголовки присутствуют
        df = df[df['headline.main'].isnull() == False]

    # Убираю статьи с субъективными мнениями авторов
    df = df[df['type_of_material'] != 'op-ed']

    # Оставляю нужные поля
    df = df[['headline.main', 'pub_date', 'byline.original', 'web_url']]

    df.columns = ['headline', 'date', 'author', 'url']

    return df

def load(df: pd.DataFrame):
    """Загрузка в БД."""
    engine = create_engine(DATABASE_URL)
    df.to_sql(name='new_articles',
                   con=engine,
                   index=False,
                   if_exists='append')


if __name__ == '__main__':
    df = extract()
    df = transform(df)
    load(df)