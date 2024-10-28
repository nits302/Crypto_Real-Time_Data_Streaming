import uuid
import datetime
import requests
import json
import time
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer


# Hàm lấy dữ liệu từ API
def get_data():
    url = "https://api-invest.goonus.io/api/v1/currency?baseCurrency=USDT"
    res = requests.get(url)
    res = res.json()
    return res


# Hàm định dạng dữ liệu thành một danh sách các bản ghi phù hợp
def format_data(res):
    data_list = []
    for crypto in res["data"]:
        rank = crypto.get("rank")
        if rank and 1 <= rank <= 10:
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


# Hàm gửi dữ liệu tới Kafka
def stream_data():
    # Cấu hình logging để dễ dàng kiểm tra lỗi
    logging.basicConfig(level=logging.INFO)

    # Khởi tạo Kafka Producer
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    curr_time = time.time()

    # Vòng lặp để gửi dữ liệu trong khoảng thời gian 1 phút
    while True:
        if time.time() > curr_time + 120:  # 1 minute
            break
        try:
            # Lấy và định dạng dữ liệu
            res = get_data()
            data_list = format_data(res)

            # Gửi từng bản ghi trong data_list tới Kafka
            for data in data_list:
                producer.send("cryptos_created", json.dumps(data).encode("utf-8"))
                logging.info(f"Data sent to Kafka: {data}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue
    producer.close()


# Thiết lập thông số mặc định cho DAG
default_args = {
    "owner": "airscholar",
    "start_date": datetime.datetime(2023, 9, 3, 10, 00),
    "retries": 1,
}
# Định nghĩa DAG trong Airflow
with DAG(
    "crypto_automation",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    # Thiết lập task PythonOperator để chạy hàm stream_data
    streaming_task = PythonOperator(
        task_id="stream_data_crypto", python_callable=stream_data
    )

# res = get_data()
# result = format_data(res)
# print(result)
# stream_data()
