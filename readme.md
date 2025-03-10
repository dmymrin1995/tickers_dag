# файлы проекта
* `tickers_dag.py` - даг для получения данных с апи, записи их в базу данных, проверки качества данных
* `tickers_db_dump.sql` - дамп базы данных
* `avg_new_york_tickets.sql` - запрос к базе для поиска среднего колебания цен в сентябре по тикерам из нью-йока
* `requirements.txt` - python зависимости
* `data_quality.yaml` - запросы для проверки качества данных
* `avg_price_range.csv` - результаты подсчета колебаний цены в сентябре

Запускал все на своей арендованой машинке.

# план работы
Создание переменных для работы
```bash
    airflow variables set marketstack_api_key 123*****
    airflow variables set TICKERS_LIST_FILE "/home/dmymrin/tickers_list_data.json"
    airflow variables set DATA_QUALITY_QUERYES "/home/dmymrin/airflow/dags/tickers/data_quality.yaml"
    airflow variables set TICKER_INFO_HOME "/home/dmymrin/tickers/info"
    airflow variables set TICKERS_EOD_HOME "/home/dmymrin/tickers/eod_history"
    airflow variables set AVG_RANNGE_CSV_PATH "/home/dmymrin/airflow/dags/tickers/"
```
План работы:
1. Загрузить данные из api и сохранить в файловой системе.
2. Создать базу данных. (вне DAG)
3. Загрузить в базу данные их json файлов.
4. Проверить качество данных
5. Посчитать среднее значение колебания цен по каждому тикеру из Нью Йорка за сентябрь 2024 года, сохранить в csv

# описание загрузки данных с апи
за загрузку данных из апи отвечют `ticker_info_task` и `ticker_eod_hist_task`. т.к. мы загружаем исторические данные поэтому для расписания dag должен быть выполнен 1 раз. таски это `PythonOperator` которые в свою очередь выполняют запуск функций `_get_ticker_info` и `_get_ticker_eod_data`. внутри функций для запросов используются `requests`. т.к. ограничение по запросам ограничено 100 запросами в месяц, было принято решение разделить это колличество пополам что бы получить ориентировачно 50 tickers info и 50 eod. список сиволов для тикеров предварительно был загружен с эндпойнта `api.marketstack.com/v2/tickerslist`.

# база даннных
проектировал базу данных на сайте [drawdb](https://drawdb.vercel.app/), по эотому там есть необычные конструкции. изучив данные и отталкиваясь от задачи принял решение создать 3 таблицы:
* ticker_info - информация по тикерам
* end_of_day_data - End-of-day данные
* exchanges - данные о биржах

`ticker_info` - позволяет идентифицировать компанию, сектор экономики, обозначение тикера на торгах. для ее создания были использованы следующие данные:

```json
{
    "name": "Adobe Inc.",
    "ticker": "ADBE",
    "item_type": "equity",
    "sector": "Technology",
    "industry": "Software—Infrastructure",
    "exchange_code": "NMS",
    [...],
    "stock_exchanges": [
        {
            "city": "New York",
            "country": "USA",
            "website": "www.iextrading.com",
            "acronym1": "IEX",
            "alpha2_code": "US",
            "exchange_mic": "IEXG",
            "exchange_name": "Investors Exchange"
        },
        {
            "city": "NEW YORK",
            "country": "",
            "website": "www.nasdaq.com",
            "acronym1": "NASDAQ",
            "alpha2_code": "US",
            "exchange_mic": "XNAS",
            "exchange_name": "NASDAQ - ALL MARKETS"
        },
        {
            "city": "New York",
            "country": "USA",
            "website": "www.nasdaq.com",
            "acronym1": "NASDAQ",
            "alpha2_code": "US",
            "exchange_mic": "XNAS",
            "exchange_name": "NASDAQ Stock Exchange"
        }
    ]
}

```
для того что бы понят в каком городе торгуется тикер необходимо заранее знать сивольное обозначение биржи, однако посмотрев на данные я решил собрать данные о биржах в отдельную таблицу. `exchanges` - таблица с данными о биржах, необходимыми для того что бы понять в каком городе торгуется тикер. для создания таблицы используется значение ключа `stock_exchanges`, который список информации на каких площадках торгуется тикер.<br>
`end_of_day_data` данные о результатов торгов. для `end_of_day_data` использовались ключи:
```json
{
        "open": 235.495,
        "high": 236.55,
        "low": 229.23,
        "close": 235.74,
        "volume": 46969202.0,
        "adj_high": 236.55,
        "adj_low": 229.23,
        "adj_close": 235.74,
        "adj_open": 235.42,
        "adj_volume": 46987098.0,
        "split_factor": 1.0,
        "dividend": 0.0,
        "exchange_code": "NASDAQ",
        "asset_type": "Stock",
        "price_currency": "usd",
        "symbol": "AAPL",
        "exchange": "XNAS",
        "date": "2025-03-05T00:00:00+0000"
    }

```
проще говоря почти все кроме  `name` т.к. он уже есть в `ticker_info`. поле в `symbol` связывает таблицу с таблицей информации по тикеру. поле `exchange` связывает таблицу с таблицей с информацией по биржам.<br>

Заполнение базы реализованы в DAG через PosgreHook. Для избежания создания дубликатов запрос учитывет возникновение конфликтов
```sql
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
```

# проверка качества данных
в голову пришла идея сделать проверки на подобии тестов из dbt, т.е. собрать запросы для проверки в yaml файл.
1. проверить что в таблице `tickers_info` нет добликатов при загрузке
```sql
SELECT 
    ticker, 
    count(ticker) 
FROM tickers_info 
GROUP BY ticker 
HAVING count(ticker) > 1;
```
2. т.к. нам необходимо использовать данные городе в котором находится тикер, необходимо проверить что в поле `city` в таблице `exchanges` не содержит `NULL`
```sql
SELECT count(*) 
FROM exchanges 
WHERE city IS NULL;
```

3. что бы получить корректные данные о колебании цен на бирже нужно проверить что данные не содержат аномалий. что минимальная цена не должна быть выше чем максимальная. цена на открытии торгов и на закрытии не должна быть меньше нуля
```sql
SELECT 
    symbol, 
    "date", 
    "open", 
    high, 
    low, 
    "close" 
FROM end_of_day_data 
WHERE high < low 
    OR "open" < 0 
    OR "close" < 0
```
4. Если данные о торгах будут содержать дубликаты результат будет некорректным, поэтому их тоже необходимо проерить на дубликаты.
```sql
SELECT 
    symbol, 
    "date", 
    COUNT(*) 
FROM end_of_day_data 
GROUP BY symbol, "date"
HAVING COUNT(*) > 1;
```
