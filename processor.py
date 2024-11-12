import sys
import os
import json
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Configuration
from pyflink.common.typeinfo import Types

# Установим переменные для Kafka
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "sensor_data"
OUTPUT_TOPIC = "processed_data"
GROUP_ID = "flink-consumer-group"

# Настройка окружения Flink
env = StreamExecutionEnvironment.get_execution_environment()

# Настройка Kafka Consumer с использованием confluent_kafka
consumer_config = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
}

# Подключение к Kafka через Flink
consumer = FlinkKafkaConsumer(
    topics=INPUT_TOPIC,
    deserialization_schema=SimpleStringSchema(),
    properties=consumer_config,
)


# Пайплайн обработки данных
def process_data(record):
    # Преобразование данных (например, в верхний регистр)
    return record.upper()


# Основная логика обработки потока
stream = env.add_source(consumer)
processed_stream = stream.map(process_data, output_type=Types.STRING())

# Настройка Kafka Producer с использованием confluent_kafka
producer_config = {"bootstrap.servers": KAFKA_BROKER}


# Создание Kafka Producer через confluent_kafka
def send_to_kafka(record):
    producer = Producer(producer_config)

    # Отправка сообщений в Kafka
    def delivery_report(err, msg):
        if err is not None:
            print("Message delivery failed: {}".format(err))
        else:
            print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))

    producer.produce(OUTPUT_TOPIC, record.encode("utf-8"), callback=delivery_report)
    producer.flush()


# Запись обработанных данных в Kafka
processed_stream.add_sink(send_to_kafka)

# Запуск процесса
env.execute("Flink Kafka Stream Processing")
