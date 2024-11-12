from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, window, avg
from pyspark.streaming import StreamingContext
from confluent_kafka import Producer, Consumer
import json
from datetime import datetime

# Налаштування Spark
spark = SparkSession.builder.appName("SensorDataStream").getOrCreate()

ssc = StreamingContext(spark.sparkContext, 10)  # 10 секунд для кожного мікро-інтервалу

# Налаштування Kafka
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "sensor_data"
OUTPUT_TOPIC = "sensor_alerts"
GROUP_ID = "sensor-consumer-group"

consumer_config = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
}

producer_config = {
    "bootstrap.servers": KAFKA_BROKER,
}

# Створення Kafka consumer та producer
consumer = Consumer(consumer_config)
producer = Producer(producer_config)

# Читання умов для алертів з CSV
import pandas as pd

alerts_conditions = pd.read_csv("./data/alerts_conditions.csv")


# Функція для обробки потоку
def process_stream(rdd):
    if not rdd.isEmpty():
        # Перетворюємо RDD на DataFrame
        df = spark.read.json(rdd)

        # Преобразуем время
        df = df.withColumn("timestamp", unix_timestamp("timestamp").cast("timestamp"))

        # Використовуємо агрегацію з вікном розміром 1 хвилина, з інтервалом 30 секунд
        agg_df = df.groupBy(window(df.timestamp, "1 minute", "30 seconds")).agg(
            avg("temperature").alias("t_avg"), avg("humidity").alias("h_avg")
        )

        # Перевіряємо умови для алертів
        alerts = []
        for row in agg_df.collect():
            for _, condition in alerts_conditions.iterrows():
                if (
                    (
                        condition["temperature_min"] != -999
                        and row["t_avg"] < condition["temperature_min"]
                    )
                    or (
                        condition["temperature_max"] != -999
                        and row["t_avg"] > condition["temperature_max"]
                    )
                    or (
                        condition["humidity_min"] != -999
                        and row["h_avg"] < condition["humidity_min"]
                    )
                    or (
                        condition["humidity_max"] != -999
                        and row["h_avg"] > condition["humidity_max"]
                    )
                ):
                    alert = {
                        "window_start": row["window"].start,
                        "window_end": row["window"].end,
                        "t_avg": row["t_avg"],
                        "h_avg": row["h_avg"],
                        "code": condition["code"],
                        "message": condition["message"],
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    alerts.append(alert)

        # Відправка алертів у Kafka
        for alert in alerts:
            producer.produce(OUTPUT_TOPIC, value=json.dumps(alert))
        producer.flush()


# Читання з Kafka та перетворення на DStream
stream = ssc.kafkaStream(KAFKA_BROKER, INPUT_TOPIC, consumer_config)

# Обробка потоку
stream.foreachRDD(process_stream)

# Запуск Spark Streaming
ssc.start()
ssc.awaitTermination()
