import os
import json
import yaml
import logging
import requests
from glob import glob
from pathlib import Path
from datetime import datetime

import pandas as pd
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

TICKERS_LIST_FILE = "/home/dmymrin/tickers_list_data.json"
DATA_QUALITY_QUERYES = "/home/dmymrin/airflow/dags/tickers/data_quality.yaml"
TICKER_INFO_HOME = "/home/dmymrin/tickers/info"
TICKERS_EOD_HOME = "/home/dmymrin/tickers/eod_history"
AVG_RANNGE_CSV_PATH = "/home/dmymrin/airflow/dags/tickers/"
ACCESS_KEY = Variable.get('marketstack_api_key')

def _get_ticker_info(ticker_symbol: str):
    """
    Функция выполняет запрос к апи к эндпойнту `api.marketstack.com/v2/tickerinfo`
    Сохраняет данные в файловую систему
    Args:
        `ticker_symbol`: str - символ идентефикатор тикера
    Returns:
        None
    """

    ticker_info_home_folder = TICKER_INFO_HOME

    Path(ticker_info_home_folder).mkdir(parents=True, exist_ok=True)
    
    try:
        tickers_info_responce = requests.get(
            f"http://api.marketstack.com/v2/tickerinfo"\
            f"?access_key={ACCESS_KEY}&ticker={ticker_symbol}"
        )
        
        if not tickers_info_responce.status_code == 200:
            logging.warning(f"Запрос для символа {ticker_symbol} не выполнен "\
                f"статус ответа {tickers_info_responce.status_code}")
            return
        
        tickers_info_responce_data: dict = json.loads(tickers_info_responce.content)

        """
        Согласно документации api от эндпойнта может прийти ответ со статусом 200 
        однако в ответе может содержаться ошибка
        ```
        {
            "error": {
                "code": "validation_error",
                "message": "Request failed with validation error",
                "context": {
                    "symbols": [
                        {
                        "key": "missing_symbols",
                        "message": "You did not specify any symbols."
                        }
                    ]
                }
            }
        }

        ```
        поэтому необходимо проверять не только status code но и наличие ошибки в ключах

        """
        
        if "error" in tickers_info_responce_data.keys():
            logging.warning(f"Ошибка в данных API: {tickers_info_responce_data.get('error')}")
            return

        tickers_info_data = tickers_info_responce_data.get('data')

            # проверяем что список не пустой
        if not tickers_info_data:
            logging.warning(
                f"Инофрмация для символа {ticker_symbol} пуста"
            )
            return
        # объявляем путь к файлу в который сохраним информацию о тикере
        ticker_info_filename = os.path.join(
            ticker_info_home_folder, f'{ticker_symbol}_info_data.json'
        )
                
        # сохраняем информацию в файл
        with open(ticker_info_filename, 'w', encoding='utf-8') as file:
            json.dump(tickers_info_data, file, ensure_ascii=False, indent=4)

        logging.info(f"Информация для символа: {ticker_symbol}, схранена")
    
    except Exception as e:
        logging.error(f'Ошибка {e} для символа {ticker_symbol}')

def _get_ticker_eod_data(symbols: str, date_from: datetime, limit: int = 1000):
    """
    Функция для получения исторической ифнормации End-of-Day Data для кокретного
    тикера выполняет запрос к эндпойнту `api.marketstack.com/v2/eod`
    Сохраняет данные в файловую систему
    
    Args:
        symbols: str - "символ", уникальный идентефикатор тикера на бирже
        date_from: datetime - начальная дата с которой мы получаем данные
        limit: int - колличество запичей которые мы получим с эндпойнта, 
            максимальное колличетсво 1000. Значение по умолчанию 1000.
    
    Returns:
        None
    
    """
    
    ticker_eod_home_folder = TICKERS_EOD_HOME
    Path(ticker_eod_home_folder).mkdir(parents=True, exist_ok=True)
    date_from_iso = date_from.strftime('%Y-%m-%d')
    
    try:
        logging.info(f"Выполняется запрос для символа {symbols}")
        ticker_eod_responce = requests.get("http://api.marketstack.com/v2/"\
            f"eod?access_key={ACCESS_KEY}"\
            f"&symbols={symbols}&date_from={date_from_iso}&limit={limit}"\
        )
        
        if not ticker_eod_responce.status_code == 200:
            logging.warning(f"Запрос для символа {symbols} не выполнен "\
                f"статус ответа {ticker_eod_responce.status_code}")
            return
        
        tickers_responce_eod_data: dict = json.loads(ticker_eod_responce.content)
        
        if "error" in tickers_responce_eod_data.keys():
            logging.warning(
                f"Ошибка в данных API: {tickers_responce_eod_data.get('error')}"
            )
            return

        tickers_eod_data = tickers_responce_eod_data.get('data')
        
        if not tickers_eod_data:
            logging.warning(
                f"Инофрмация End-of-Day для символа {symbols} пуста"
            )
            return
        
        ticker_eod_data_filename = os.path.join(
            ticker_eod_home_folder, f'{symbols}_EOD_data.json'
        )
        
        with open(ticker_eod_data_filename, 'w', encoding='utf-8') as file:
            json.dump(tickers_eod_data, file, ensure_ascii=False, indent=4)
            
        logging.info(f"Информация для символа {symbols} сохранена")

    except Exception as e:
        print(f"Error {e}")

def _insert_ticker_info():
    """
    Добавить данные о тикерах из json файла в базу данных.
    Одновременно заполняет информацию в таблицу `ticker_info` и `exchanges` 
    т.к. информация содержится для обеих таблиц содержится в одном файле.
    """

    # получить список всех файлов в папке info
    files_path = glob('/home/dmymrin/tickers/info/*.json')
    pg_hook = PostgresHook(postgres_conn_id='tickers_db_connection')

    for file_path in files_path:
        with open(file_path, 'r') as f:
            data = json.load(f)

        # запрос на добавление данных в таблицу tickers_info, только уникальных имен тикеров
        insert_ticker_info = """
            INSERT INTO tickers_info (name, ticker, item_type, sector, industry, exchange_code)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker) DO NOTHING
        """

        # получить значения из файла с информацией о тикерах для таблицы ticker_info
        ticker_info = (
            data['name'], 
            data['ticker'], 
            data['item_type'], 
            data['sector'], 
            data['industry'], 
            data['exchange_code']
        )
        
        # выполнить запрос с параметрами из файла.
        pg_hook.run(insert_ticker_info, parameters=ticker_info)

        # запрос на добавление данных в таблицу exchanges
        # только уникальных международных кодов биржи
        exchange_insert_query = """
            INSERT INTO exchanges (
                exchange_mic, 
                acronym1, 
                city, 
                country, 
                website, 
                alpha2_code, 
                exchange_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (exchange_mic) DO NOTHING
            """
        
        for exchange in data['stock_exchanges']:
            exchange_values = (
                exchange['exchange_mic'], 
                exchange['acronym1'],
                exchange['city'],
                exchange['country'],
                exchange['website'],
                exchange['alpha2_code'],
                exchange['exchange_name']
            )
            pg_hook.run(exchange_insert_query, parameters=exchange_values)

def _insert_eod_data():

    """
    Собирать пути к файлам с End-of-Day Data, 
    добавить End-of-Day Data в таблицу `end_of_day_data`.
    """

    files_path = glob('/home/dmymrin/tickers/eod_history/*.json')
    pg_hook = PostgresHook(postgres_conn_id='tickers_db_connection')
    required_fields = ['symbol', 'exchange', 
        'volume', 'adj_volume', 'open', 'high', 'low', 
        'close', 'split_factor', 'date']

    for file_path in files_path:

        with open(file_path, 'r') as f:
            data = json.load(f)

        insert_query = """
            INSERT INTO end_of_day_data (
                symbol, exchange, open, high, low, close, volume, 
                adj_high, adj_low, adj_close, adj_open, adj_volume, 
                split_factor, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for entry in data:
            # Проверить что все нужные ключи не содержат none
            # Если хоть один пустой не вносить строчку в базу
            if not all(entry.get(field) is not None for field in required_fields):
                continue

            values = (
                entry['symbol'],
                entry['exchange'],
                entry['open'],
                entry['high'],
                entry['low'],
                entry['close'],
                entry.get('volume', 0),
                entry.get('adj_high', 0),
                entry.get('adj_low', 0),
                entry.get('adj_close', 0),
                entry.get('adj_open', 0),
                entry.get('adj_volume', 0),
                entry['split_factor'],
                entry['date']
            )
            pg_hook.run(insert_query, parameters=values)

def _check_data_quality():
    """
    Выполнить запросы на проверку качество данных.
    Запросы собраны в yaml файле (в качетсве референса используется подход из dbt)
    """

    pg_hook = PostgresHook(postgres_conn_id="tickers_db_connection")
    
    with open(DATA_QUALITY_QUERYES, "r") as file:
        queries = yaml.safe_load(file)

    for check, query in queries.items():
        records = pg_hook.get_first(query)
        if records:
            logging.warning(f"Проверка {check} провалена! Найдено {records[0]} ошибок.")
    
    logging.info("Все проверки качества данных пройдены успешно!")

def _get_avg_price_range():
    pg_hook = PostgresHook(postgres_conn_id="tickers_db_connection")

    query = """
     SELECT                   
        eod.symbol AS ticker,
        AVG(eod.high - eod.low) AS avg_price_fluctuation
    FROM end_of_day_data eod
    JOIN tickers_info t ON eod.symbol = t.ticker 
    JOIN exchanges ex ON eod.exchange = ex.exchange_mic
    WHERE
        lower(ex.city) = 'new york'
        AND eod.date >= '2024-09-01'
        AND eod.date <= '2024-09-30'
    GROUP BY
        eod.symbol
    """
    result = pg_hook.get_pandas_df(query)

    csv_path = os.path.join(
        AVG_RANNGE_CSV_PATH, 'avg_price_range.csv'
    )

    result.to_csv(csv_path, index=False)

def get_tickers(responce_limit: int = 50):
    
    """
        Функция позволяющая выбрать `responce_limit` колличество тикеров, 
        заранее загруженных имен тикеров.
        
        Args:
            responce_limit: int - ограничение на колличество тикеров из списка
                по умолчанию 50

    """
    
    with open(TICKERS_LIST_FILE, "r", encoding="utf-8") as file:
            tickers = json.load(file)
    tickers = [ticker.get('ticker') for ticker in tickers]
    
    return tickers[:responce_limit]


with DAG(
    dag_id='fetch_ticker_historical_data',
    start_date=datetime(2025, 3, 5),
    schedule='@once',
) as dag:
    
    """
        Так как требуются исторические данные DAG должен быть запущен единожды.    
    """
    
    # получить список тикеров
    tickers = get_tickers(40)
    
    # создать задачи на на получение информации по тикеру и End-of-Day Data
    # сложить полученную информацию в json
    for ticker in tickers:
        
        ticker_info_task = PythonOperator(
            task_id=f"{ticker}_ticker_info_task",
            python_callable=_get_ticker_info,
            op_args=[ticker],
        )
        
        ticker_eod_hist_task = PythonOperator(
            task_id=f"{ticker}_ticker_eod_hist_task",
            python_callable=_get_ticker_eod_data,
            op_args=[ticker, datetime(2024, 1, 1)],
        )

        ticker_info_task >> ticker_eod_hist_task
        
    # загрузка данных в PostgreSQL
    # собрать список json файлов собранных внутри тасок.

    insert_ticker_info_task = PythonOperator(
        task_id=f"insert_ticker_info_task",
        python_callable=_insert_ticker_info,
    )
    
    insert_eod_task = PythonOperator(
        task_id=f"insert_eod_task",
        python_callable=_insert_eod_data,
    )

    """
    Проверка качества данных:
    1. Проверить таблицу tickers_info на дубликаты
    2. Проверить поле city в таблице exchanges  на отсутсвие дубликатов
    3. Проверить аномалии в таблице end_of_day_data. 
        Поля high > low, open > 0, close > 0
    4. Проверить дубликаты end_of_day_data

    """
    quality_check_task = PythonOperator(
        task_id="quality_check_task",
        python_callable=_check_data_quality,
        dag=dag
    )
    
    avg_price_range_task = PythonOperator(
        task_id="avg_price_range_task",
        python_callable=_get_avg_price_range,
    )
    
    insert_ticker_info_task >> insert_eod_task >> quality_check_task >> quality_check_task >> avg_price_range_task