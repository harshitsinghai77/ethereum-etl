"""Before running this script, ensure that you have a Kafka broker running on localhost:9092"""
"""use "make docker-compose-up" on linux to start a Kafka broker on localhost:9092"""
"""This script will read data from the Kafka topic 'transactions' and print it to the console"""

from pyspark.sql import SparkSession

from constants import KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC

scala_version = "2.12"
spark_version = "3.1.2"
# TODO: Ensure match above values match the correct versions
packages = [
    f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
    "org.apache.kafka:kafka-clients:3.2.1",
]
spark = (
    SparkSession.builder.appName("EthereumETL")
    .config("spark.jars.packages", ",".join(packages))
    .getOrCreate()
)


# Reference: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html

# Read data from Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:{KAFKA_PORT}")
    .option("subscribe", KAFKA_TOPIC)
    .option("batchDuration", 300)
    .load()
)

# Write data to console
query = (
    df.selectExpr("CAST(value AS STRING)")
    .writeStream.outputMode("append")
    .format("console")
    .start()
)

query.awaitTermination()
