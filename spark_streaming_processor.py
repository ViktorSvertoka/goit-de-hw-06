from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from config import kafka_config

# Створення SparkSession
spark = (
    SparkSession.builder.appName("SensorDataProcessing")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .getOrCreate()
)

# Опис схеми CSV
schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("temperature", FloatType(), True),
        StructField("humidity", FloatType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

# Читання CSV файлу
sensor_df = spark.readStream.schema(schema).csv(
    "./data"
)  # Указываем папку, где лежит ваш CSV

# Преобразование timestamp из Long в Timestamp
sensor_df = sensor_df.withColumn(
    "timestamp", from_unixtime("timestamp").cast("timestamp")
)

# Агрегація даних за допомогою Sliding Window
windowed_df = (
    sensor_df.withWatermark("timestamp", "10 seconds")
    .groupBy(window("timestamp", "1 minute", "30 seconds"))
    .agg(avg("temperature").alias("t_avg"), avg("humidity").alias("h_avg"))
)

# Запис результатів в Kafka
query = (
    windowed_df.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("topic", kafka_config["output_topic"])
    .outputMode("append")
    .start()
)

query.awaitTermination()
