import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from confluent_kafka import Producer
import json
import time
import random
from config import kafka_config
from colorama import Fore, init


# Ініціалізація colorama
init(autoreset=True)

# Налаштування Kafka Producer
producer = Producer(
    {
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": kafka_config["security_protocol"],
        "sasl.mechanism": kafka_config["sasl_mechanism"],
        "sasl.username": kafka_config["username"],
        "sasl.password": kafka_config["password"],
    }
)


# Функція для генерації даних сенсора
def generate_data():
    sensor_id = random.randint(1, 100)
    temperature = random.uniform(10, 50)
    humidity = random.uniform(10, 90)
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    return {
        "sensor_id": str(sensor_id),
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": timestamp,
    }


# Надсилання даних у Kafka
topic = kafka_config["input_topic"]
count = 0
max_count = 10  # Максимальна кількість повідомлень

while count < max_count:
    data = generate_data()

    # Виведення даних з кольорами
    print(f"{Fore.CYAN}Sent:")
    print(f"  {Fore.YELLOW}sensor_id{Fore.RESET}: {Fore.GREEN}{data['sensor_id']}")
    print(
        f"  {Fore.YELLOW}temperature{Fore.RESET}: {Fore.RED}{data['temperature']:.2f}"
    )
    print(f"  {Fore.YELLOW}humidity{Fore.RESET}: {Fore.MAGENTA}{data['humidity']:.2f}")
    print(f"  {Fore.YELLOW}timestamp{Fore.RESET}: {Fore.BLUE}{data['timestamp']}")
    print(" ")

    # Відправка даних у Kafka
    producer.produce(topic, key=data["sensor_id"], value=json.dumps(data))
    producer.flush()

    time.sleep(2)
    count += 1

print(" ")

print("Sent 10 messages, exiting.")

print(" ")
