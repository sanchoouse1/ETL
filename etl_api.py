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

df = pd.DataFrame()

current_date = datetime.datetime.now().strftime('%Y%m%d')
page_num = 1

load_dotenv()
API_KEY = os.getenv('API_KEY')
