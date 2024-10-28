from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import logging
from kafka import KafkaProducer
import psycopg2
import requests
import time

# Định nghĩa các tham số mặc định cho DAG
default_args = {"owner": "airscholar", "start_date": datetime(2024, 10, 23, 10, 00)}

# Định nghĩa DAG
dag = DAG(
    "crypto_stream_dag",
    default_args=default_args,
    description="A DAG to stream crypto data",
    schedule_interval=timedelta(days=1),  # Sửa lại phần schedule_interval
)


# Task 1: Fetch dữ liệu từ API
def get_data():
    print("Fetching data from API")
    res = requests.get("https://api-invest.goonus.io/api/v1/currency?baseCurrency=USDT")
    if res.status_code == 200:
        return res.json()
    else:
        logging.error("Failed to fetch data")
        return None


# Task 2: Format lại dữ liệu
def format_data(**kwargs):
    print("Formatting data")
    data = kwargs["ti"].xcom_pull(task_ids="get_data")
    formatted_data = []
    for crypto in data["data"]:
        rank = crypto.get("rank")
        if rank and 1 <= rank <= 10:
            symbol = crypto.get("symbol")
            name = crypto.get("name")
            price = crypto["statistics"].get("price")
            price_change_24h = crypto["statistics"].get("priceChangePercentage24h")
            volume = crypto.get("volume")
            volume_24h = crypto["statistics"].get("volume")
            volume_change_24h = crypto.get("volumeChangePercentage24h")
            market_cap = crypto["statistics"].get("marketCap")
            updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            formatted_data.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "rank": rank,
                    "price": price,
                    "price_change_24h": price_change_24h,
                    "volume": volume,
                    "volume_24h": volume_24h,
                    "volume_change_24h": volume_change_24h,
                    "market_cap": market_cap,
                    "updated_at": updated_at,
                }
            )
    return formatted_data


# Task 3: Lưu dữ liệu vào PostgreSQL
def store_data(**kwargs):
    print("Storing data")
    data = kwargs["ti"].xcom_pull(task_ids="format_data")
    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        database="airflow",
        user="airflow",
        password="airflow",
    )
    cur = conn.cursor()
    for crypto in data:
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

        insert_query = """
            INSERT INTO crypto_data (
                symbol, name, rank, price, price_change_24h,
                volume, volume_24h, volume_change_24h,
                market_cap, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(
            insert_query,
            (
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
    cur.close()
    conn.close()
    print("Successfully stored data")


def stream_data(**kwargs):
    print("Streaming data to Kafka")
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:  # Dừng sau 1 phút
            break
        try:
            # Fetch dữ liệu mới từ API và format lại
            res = get_data()
            formatted_data = format_data(res)

            # Stream dữ liệu đến Kafka topic "crypto_data"
            producer.send("crypto_data", json.dumps(formatted_data).encode("utf-8"))

            # Nghỉ 5 giây trước khi lấy dữ liệu tiếp theo
            time.sleep(5)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue

    producer.flush()
    producer.close()
    print("Successfully streamed data")


# Định nghĩa các task
task_get_data = PythonOperator(
    task_id="get_data",
    python_callable=get_data,
    dag=dag,
)

task_format_data = PythonOperator(
    task_id="format_data",
    python_callable=format_data,
    provide_context=True,
    dag=dag,
)

task_store_data = PythonOperator(
    task_id="store_data",
    python_callable=store_data,
    provide_context=True,
    dag=dag,
)

task_stream_data = PythonOperator(
    task_id="stream_data",
    python_callable=stream_data,
    provide_context=True,
    dag=dag,
)

# Định nghĩa thứ tự thực hiện các task
task_get_data >> task_format_data >> task_store_data
task_get_data >> task_format_data >> task_stream_data
