from kafka import KafkaConsumer
from configs import kafka_config
import json
from topic_configs import ALERTS_TOPIC

consumer = KafkaConsumer(
    ALERTS_TOPIC,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda v: v.decode("utf-8") if v else None,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="consumer_group"
)

print(f"Subscribed to topic '{ALERTS_TOPIC}'")

try:
    for message in consumer:
        data = message.value
        print(f"Received alert: {data}, partition {message.partition}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    consumer.close()