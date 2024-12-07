import uuid
import datetime
import requests
import json
import logging
import time
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)


# https://api-invest.goonus.io/api/v1/currency?baseCurrency=USDT
def get_data():
    url = "https://api-invest.goonus.io/api/v1/currency?query=&tag="
    res = requests.get(url)
    res = res.json()
    return res


def format_data(res):
    data_list = []
    for crypto in res["data"]:
        rank = crypto.get("rank")
        if rank and 1 <= rank <= 11:
            data = {
                "id": str(uuid.uuid4()),
                "symbol": crypto.get("symbol"),
                "name": crypto.get("name"),
                "rank": crypto.get("rank"),
                "price": crypto["statistics"].get("price"),
                "price_change_24h": crypto["statistics"].get(
                    "priceChangePercentage24h"
                ),
                "volume": crypto.get("volume"),
                "volume_24h": crypto["statistics"].get("volume"),
                "volume_change_24h": crypto.get("volumeChangePercentage24h"),
                "market_cap": crypto["statistics"].get("marketCap"),
                "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            data_list.append(data)
    return data_list


def storage_data():
    res = get_data()
    data = format_data(res)
    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        database="airflow",
        user="airflow",
        password="airflow",
    )
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cryptos (
            id UUID PRIMARY KEY,
            symbol VARCHAR(10),
            name VARCHAR(100),
            rank INTEGER,
            price NUMERIC,
            price_change_24h NUMERIC,
            volume NUMERIC,
            volume_24h NUMERIC,
            volume_change_24h NUMERIC,
            market_cap NUMERIC,
            updated_at TIMESTAMP
        )
    """
    )

    for crypto in data:
        id = crypto["id"]
        symbol = crypto["symbol"]
        name = crypto["name"]
        rank = crypto["rank"]
        price = crypto["price"]
        price_change_24h = crypto["price_change_24h"]
        volume = crypto["volume"]
        volume_24h = crypto["volume_24h"]
        volume_change_24h = crypto["volume_change_24h"]
        market_cap = crypto["market_cap"]
        updated_at = crypto["updated_at"]
        cur.execute(
            """
            INSERT INTO cryptos (id, symbol, name, rank, price,
            price_change_24h, volume, volume_24h,
            volume_change_24h, market_cap, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                id,
                symbol,
                name,
                rank,
                price,
                price_change_24h,
                volume,
                volume_24h,
                volume_change_24h,
                market_cap,
                updated_at,
            ),
        )
    conn.commit()
    conn.close()
    logging.info("Data stored successfully!")


def stream_data():
    logging.basicConfig(level=logging.INFO)

    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 600:
            break
        try:
            res = get_data()
            data_list = format_data(res)

            for data in data_list:
                producer.send("cryptos_created", json.dumps(data).encode("utf-8"))
                logging.info(f"Data sent to Kafka: {data}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue
    producer.close()


default_args = {
    "owner": "airscholar",
    "start_date": datetime.datetime(2024, 9, 3, 10, 00),
    "retries": 1,
}

with DAG(
    "crypto_automation",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    storage_task = PythonOperator(
        task_id="store_data_crypto",
        python_callable=storage_data,
    )
    streaming_task = PythonOperator(
        task_id="stream_data_crypto", python_callable=stream_data
    )

    storage_task >> streaming_task
