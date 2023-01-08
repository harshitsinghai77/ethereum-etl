"""Before running this script, ensure that you have a Kafka broker running on localhost:9092"""
"""use "make docker-compose-up" on linux to start a Kafka broker on localhost:9092"""
"""Send data from a PySpark DataFrame to a Kafka topic"""

from pyspark.sql import SparkSession
from kafka import KafkaProducer

from constants import KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC

# Create spark session
spark = SparkSession.builder.appName("EthereumETL").getOrCreate()

# Create a KafkaProducer
producer = KafkaProducer(bootstrap_servers=[f"{KAFKA_HOST}:{KAFKA_PORT}"])

# Send data from a PySpark DataFrame to a Kafka topic
def send_to_kafka(df, topic):
    # Convert the DataFrame to a list of dictionaries
    records = df.toJSON().collect()

    # Send the records to the Kafka topic
    for record in records:
        producer.send(topic, record.encode())

    # Flush the producer to ensure the data is sent
    producer.flush()

    print("Sent {} records to the {} topic".format(len(records), topic))


df = spark.read.parquet("transactions.parquet")
send_to_kafka(df, KAFKA_TOPIC)
