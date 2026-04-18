from kafka import KafkaProducer
from configs import kafka_config
import json
import time
import uuid
import random
from topic_configs import BUILDING_TOPIC

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None
)

sensor_id = str(uuid.uuid4())

try:
    while True:
        data = {
            "id": sensor_id,
            "temperature": round(random.uniform(10, 80), 2),
            "humidity": round(random.uniform(10, 90), 2),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        producer.send(BUILDING_TOPIC, key=sensor_id, value=data)
        producer.flush()

        print(f"Sent: {data}")
        time.sleep(2)

except KeyboardInterrupt:
    print("Producer stopped by user.")

finally:
    producer.close()