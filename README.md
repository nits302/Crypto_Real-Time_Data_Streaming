# Transforming Crypto Insights: Real-Time Data Streaming with Kafka, Spark, PostgreSQL, Docker, Cassandra, and Grafana

In the fast-paced world of cryptocurrency, real-time data streaming is essential for tracking market trends, predicting price fluctuations, and making timely decisions. This project harnesses the power of cutting-edge technologies like <b>Kafka</b>, <b>Spark</b>, <b>PostgreSQL</b>, <b>Docker</b>, <b>Cassandra</b> and <b>Grafana</b> to build an efficient, scalable system for streaming and analyzing crypto data in real time.

<p align="center">
  <img src="images/background.png" alt="Wallpaper">
</p>

## 💥 Challenge

The volatile and dynamic nature of crypto markets presents significant challenges:

- <b> Outdated Insights: Manually updating data or relying on traditional systems results in delays and missed opportunities in fast-changing markets.

- <b> Data Volume and Performance: Exponential growth in crypto data overwhelms legacy systems, leading to slow processing and higher costs.

- <b> Data Integration Issues: Aggregating data from multiple exchanges often results in inconsistencies and poor data quality, delaying actionable insights.

- <b> Monitoring and Latency: Without real-time monitoring, data pipeline issues such as data loss or corruption can go undetected, further impacting decision-making.

- <b> Storage Bottlenecks: Inefficient storage systems struggle to handle high-throughput data, increasing retrieval latency and operational costs.

## Main Tasks

This project establishes a robust, real-time crypto data analysis platform, tackling the above challenges by leveraging modern data engineering tools and methodologies.

- <b> Real-Time Data Capture: <b> Ingest crypto data streams from exchanges and APIs in real time using Kafka, ensuring minimal latency.
- <b>Scalable and Distributed Architecture: <b> Utilize Spark and Cassandra to handle high data throughput and processing efficiently, ensuring scalability for growing crypto datasets.
- <b> Data Quality and Validation: <b> Employ schema management and real-time validation to maintain data accuracy and consistency.
- <b> ETL Automation: <b> Automate the data transformation process, from raw ingestion to structured analytics-ready storage in PostgreSQL, minimizing manual intervention.
- <b> Comprehensive Monitoring and Visualization: <b> Integrate Grafana for real-time dashboards, monitoring the health and performance of data streams while providing actionable market insights.

This project transforms raw crypto data into real-time actionable insights, enabling stakeholders to respond swiftly to market trends. With a scalable and efficient system, it minimizes data latency, enhances data quality, and empowers businesses to stay ahead in the competitive crypto landscape.

## Getting Started

## 📁 Repository Structure
```shell
crypto-project/
├── .idea/                   # IDE-specific settings and configurations
├── crypto_stream.py         # Main script for crypto streaming
├── dags/                    # Airflow DAGs
│   ├── get_data.py          # Script for fetching data
│   └── kafka_stream.py      # Script for handling Kafka streaming
├── docker-compose.yml       # Docker Compose configuration file
├── picture/                 # Directory for storing images
├── README.md                # Project documentation file
├── requirements.txt         # Python dependencies
├── script/                  # Shell scripts
│   └── entrypoint.sh        # Entrypoint script for Docker
└── venv/                    # Python virtual environment
    ├── Lib/                 # Libraries and packages
    └── Scripts/             # Executables for the virtual environment
```
### Project file:

- `kafka_stream.py`: [kafka_stream.py](dags/kafka_stream.py) is created to fetches user data from ONUS API, processes and streams into a Kafka topic named `crypto_created`. The DAG in <b>Apache Airflow</b> employs PythonOperator to handle the task execution.

- `crypto_streaming.py`: [crypto-streaming.py](crypto-streaming.py) is builded to create `cassandra_keyspace`, `cassandra_table`, `cassandra_connection`, `spark_connection`, `connect_to_kafka` and integration between them.

### Running project:

1- Clone the repository:

```
git clone https://github.com/nits302/Crypto-Real-Time-Data-Streaming.git
```

2- Navigate to the project directory

```
cd streaming_realtime_data
```

3- Set Up a Virtual Environment in Python

```
pip3 install virtualenv
python -m venv venv
source venv/bin/activate
```

4- Install the needed packages and libraries:

```
pip3 install -r ./requirements.txt
```

5- Install Docker, Docker compose:

```
sudo ./installdocker.sh
docker --version
docker compose version
```

6- Build docker:

```
docker compose up -d
```

Check containers in docker desktop:

<p align="center">
  <img src="images/docker_desktop.png" alt="Wallpaper">
</p>

7- Run step by step files:

```
python crypto-streaming.py
```

8- Access to airflow UI to monitor streaming process: `localhost:8080` with account: `admin`, password: `admin`

<p align="center">
  <img src="images/airflow.png" alt="Wallpaper">
</p>

9- Access to `control center UI` monitor Topic health, Procuder and consumer performance, Offset, Cluster health: `localhost:9021`

<p align="center">
  <img src="images/control-center.png" alt="Wallpaper">
</p>

10- Check data in `Cassandra` with command:

```
docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
```

<p align="center">
  <img src="images/cassandra.png" alt="Wallpaper">
</p>

### Reference:

[1]. [Realtime Data Streaming](https://www.youtube.com/watch?v=GqAcTrqKcrY)

[2]. [Cassandra and pyspark](https://medium.com/@yoke_techworks/cassandra-and-pyspark-5d7830512f19)

<b> ⚡️That's all for my project, thanks for watching. If you have any question, don't hesitate inbox me.⚡️</b>
