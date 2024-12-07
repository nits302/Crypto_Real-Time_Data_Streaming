import uuid
import datetime
import requests
from cassandra.cluster import Cluster


# Config connect Cassandra
def get_cassandra_session():
    cluster = Cluster(["localhost"])
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


def get_data():
    url = "https://api-invest.goonus.io/api/v1/currency?baseCurrency=USDT"
    res = requests.get(url)
    res = res.json()
    print("Data fetched successfully!")
    return res


def format_data(res):
    res = get_data()
    data_list = []
    for crypto in res["data"]:
        rank = crypto.get("rank")
        if rank and 1 <= rank <= 11:
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
    print("Data formated successfully!")
    return data_list


def load_data_to_cassandra(data_list):
    data_list = format_data(get_data())
    session = get_cassandra_session()
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


res = get_data()
data_list = format_data(res)
load_data_to_cassandra(data_list)
