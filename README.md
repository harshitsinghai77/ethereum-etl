## Project Description

The goal of this project is to create a short ETL pipeline that performs the following tasks:

1. Downloads a fixed set of ethereum blocks as CSV files, covering around 20K latest blocks (approx. 3 days).
2. Converts the data into a more optimized format for storage and retrieval.
3. Loads the data into Kafka for real-time analytics.
4. Loads the data into a columnar store (such as Clickhouse) for ad-hoc analysis.

### Getting Started

To extract the data

```bash
python extract_data.py
```

The script will output the data as CSV files and will also convert them to parquet files (for optimized storage and retrieval).
The following will be output of the script

1. blocks.csv
2. transactions.csv
3. contracts.csv
4. token_transfers.csv
5. blocks.parquet
6. transactions.parquet
7. token_transfers.parquet
8. contract_addresses.txt

### Real-time Analytics

We will be using PySpark and kafka.

###

The directory contains `kafka-docker-compose.yaml` file that makes it easier to get the kafka server up and running.

```bash
docker-compose -f kafka-docker-compose.yaml up
```

Once the Kafka container is up and running,
open two terminals

terminal1

```bash
python receieve-data-on-kafka.py
```

keeping the first terminal open, on another terminal

```bash
python send-data-to-kafka.py
```

This will mimic how we can use Real-time Analytics for data processing.

### Clickhouse

If on linux.

Get Clickhouse docker image

```bash
  make get-clickhouse-docker-image
```

Run clickhouse container

```bash
make start-clickhouse-docker-container
```

If not on linux

```bash
docker pull clickhouse/clickhouse-server

docker run -d -p 18123:8123 -p19000:9000 --name clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```

### Process.ipynb

Before we start, just to make it simpler, we will be reading data from a parquet file instead of kafka topic.

1. POC for writing to kafka topic is in `send-data-to-kafka.py`
2. POC for reading from kafka topic is in `receive-data-on-kafka.py`
3. Refer to the comments in those files for more details

###

Jupyter book contains

1. Data manipulation using PySpark.
2. Creating tables using clickhouse and inserting data to clickhouse.
3. Querying Clickhouse data using SQL

### Note for the following

#### The number of ERC-20 token contracts found (the token_address field gives the address of the ERC-20 contract).

1. We will be using `ethereumetl extract_csv_column` to extract `token_address` from `token_transfers.csv`
2. The extacted `token_address` will be used to export contract using `ethereumetl export_contracts`.
3. This will export dataset `contracts.csv` with the column `is_erc20` to help us determine `How many ERC-20 token contracts were found?`

Setup
Before running the ETL pipeline, you will need to set up the following:

## Run Locally

Clone the project

```bash
  git clone https://link-to-project
```

Go to the project directory

```bash
cd etl-ethereum
```

Clickhouse deployment

```bash
  make get-clickhouse-docker-image
```

```bash
make start-clickhouse-docker-container
```

or

```bash
docker pull clickhouse/clickhouse-server

docker run -d -p 18123:8123 -p19000:9000 --name clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```

Kafka Docker container

```bash
make start-kafka-server
```

or

```bash
docker-compose -f kafka-docker-compose.yaml up
```
