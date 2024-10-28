import uuid
import datetime
import requests
import json
import time
import logging

# from airflow import DAG
# from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
from cassandra.cluster import Cluster


# Cấu hình kết nối đến Cassandra
def get_cassandra_session():
    cluster = Cluster(
        ["localhost"]
    )  # Thay "cassandra_host" bằng địa chỉ Cassandra của bạn
    session = cluster.connect()
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS crypto_datalake
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """
    )
    session.set_keyspace("crypto_datalake")
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS cryptos (
            id UUID PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            rank INT,
            price DOUBLE,
            price_change_24h DOUBLE,
            volume DOUBLE,
            volume_24h DOUBLE,
            volume_change_24h DOUBLE,
            market_cap DOUBLE,
            updated_at TIMESTAMP
        )
    """
    )
    print("Keyspace and Table created successfully!")
    return session


# Hàm lấy dữ liệu từ API
def get_data():
    url = "https://api-invest.goonus.io/api/v1/currency?baseCurrency=USDT"
    res = requests.get(url)
    res = res.json()
    print("Data fetched successfully!")
    return res


# Hàm định dạng dữ liệu thành một danh sách các bản ghi phù hợp
def format_data(res):
    data_list = []
    for crypto in res["data"]:
        rank = crypto.get("rank")
        if rank and 1 <= rank <= 10:
            data = {
                "id": uuid.uuid4(),
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
                "updated_at": datetime.datetime.now(),
            }
            data_list.append(data)
    print("Data formatted successfully!")
    return data_list


# Hàm lưu dữ liệu vào Cassandra (Data Lake)
def load_data_to_cassandra(data_list, session):
    insert_query = """
        INSERT INTO cryptos (id, symbol, name, rank, price,
        price_change_24h, volume, volume_24h,
        volume_change_24h, market_cap, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for data in data_list:
        session.execute(
            insert_query,
            (
                data["id"],
                data["symbol"],
                data["name"],
                data["rank"],
                data["price"],
                data["price_change_24h"],
                data["volume"],
                data["volume_24h"],
                data["volume_change_24h"],
                data["market_cap"],
                data["updated_at"],
            ),
        )
    print("Data loaded to Cassandra successfully!")


# Hàm đổ dữ liệu vào Cassandra mỗi 30 giây
def load_data_periodically(interval):
    session = get_cassandra_session()
    try:
        while True:
            res = get_data()
            data_list = format_data(res)
            load_data_to_cassandra(data_list, session)
            time.sleep(interval)
    finally:
        session.shutdown()


# Biến toàn cục để lưu trữ thời gian cập nhật cuối cùng
last_updated_at = datetime.datetime.min


# Hàm đọc dữ liệu từ Cassandra và stream tới Kafka
def stream_data_from_datalake():
    # Cấu hình logging
    logging.basicConfig(level=logging.INFO)

    # Khởi tạo Kafka Producer
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], max_block_ms=5000)

    # Khởi tạo kết nối Cassandra
    session = get_cassandra_session()

    # Lấy dữ liệu từ Cassandra trong 5 phút
    curr_time = time.time()
    while time.time() < curr_time + 300:  # 5 minute
        query = "SELECT * FROM cryptos WHERE updated_at > %s ALLOW FILTERING"
        rows = session.execute(query, (last_updated_at,))
        for row in rows:
            data = {
                "id": str(row.id),
                "symbol": row.symbol,
                "name": row.name,
                "rank": row.rank,
                "price": row.price,
                "price_change_24h": row.price_change_24h,
                "volume": row.volume,
                "volume_24h": row.volume_24h,
                "volume_change_24h": row.volume_change_24h,
                "market_cap": row.market_cap,
                "updated_at": row.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
            producer.send("cryptos_created", json.dumps(data).encode("utf-8"))
            logging.info(f"Data sent to Kafka: {data}")
    producer.close()
    session.shutdown()


load_data_periodically(30)
stream_data_from_datalake()
# # Thiết lập thông số mặc định cho DAG
# default_args = {
#     "owner": "airscholar",
#     "start_date": datetime.datetime(2023, 9, 3, 10, 00),
#     "retries": 1,
# }

# # Định nghĩa DAG trong Airflow
# with DAG(
#     "crypto_data_datalake_stream",
#     default_args=default_args,
#     schedule_interval="@daily",
#     catchup=False,
# ) as dag:
#     # Task 1: Lấy và định dạng dữ liệu, sau đó lưu vào Cassandra
#     load_data_task = PythonOperator(
#         task_id="load_data_to_datalake",
#         python_callable=lambda: load_data_to_cassandra(format_data(get_data())),
#     )

#     # Task 2: Đọc từ Cassandra và gửi dữ liệu tới Kafka
#     stream_data_task = PythonOperator(
#         task_id="stream_data_from_datalake", python_callable=stream_data_from_datalake
#     )

#     load_data_task >> stream_data_task  # Task Dependency
