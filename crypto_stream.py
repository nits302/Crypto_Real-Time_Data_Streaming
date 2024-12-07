import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *


def create_keyspace(session):
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS crypto_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """
    )
    print("Keyspace created successfully!")


def create_table(session):
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto_streams.created_cryptos (
            id UUID,
            symbol TEXT,
            name TEXT,
            rank INT,
            price FLOAT,
            price_change_24h FLOAT,
            volume FLOAT,
            volume_24h FLOAT,
            volume_change_24h FLOAT,
            market_cap FLOAT,
            updated_at TIMESTAMP,
            PRIMARY KEY ((symbol), updated_at)
        );
        """
    )
    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("Inserting data...")

    user_id = kwargs.get("id")
    symbol = kwargs.get("symbol")
    name = kwargs.get("name")
    rank = kwargs.get("rank")
    price = kwargs.get("price")
    price_change_24h = kwargs.get("price_change_24h")
    volume = kwargs.get("volume")
    volume_24h = kwargs.get("volume_24h")
    volume_change_24h = kwargs.get("volume_change_24h")
    market_cap = kwargs.get("market_cap")
    updated_at = kwargs.get("updated_at")

    try:
        session.execute(
            """
            INSERT INTO spark_streams.created_cryptos(id, symbol, name, rank, price, 
                price_change_24h, volume, volume_24h, volume_change_24h, market_cap, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                WHERE rank = 1;
            """,
            (
                user_id,
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
        logging.info(f"Data inserted for {symbol} with rank {rank}")

    except Exception as e:
        logging.error(f"Could not insert data due to {e}")


def create_spark_connection():
    s_conn = None
    try:
        s_conn = (
            SparkSession.builder.appName("SparkDataStreaming")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            )
            .config("spark.cassandra.connection.host", "localhost")
            .getOrCreate()
        )
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "cryptos_created")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        logging.info("Kafka DataFrame created successfully")
    except Exception as e:
        logging.warning(f"Kafka DataFrame could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        cluster = Cluster(["localhost"])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("symbol", StringType(), False),
            StructField("name", StringType(), False),
            StructField("rank", IntegerType(), False),
            StructField("price", FloatType(), False),
            StructField("price_change_24h", FloatType(), False),
            StructField("volume", FloatType(), False),
            StructField("volume_24h", FloatType(), False),
            StructField("volume_change_24h", FloatType(), False),
            StructField("market_cap", FloatType(), False),
            StructField("updated_at", TimestampType(), False),
        ]
    )

    sel = (
        spark_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    print(sel)
    return sel


if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            logging.info("Streaming is being started...")

            streaming_query = (
                selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                .option("checkpointLocation", "/tmp/checkpoint")
                .option("keyspace", "crypto_streams")
                .option("table", "created_cryptos")
                .start()
            )

            streaming_query.awaitTermination()
