from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config
from topic_configs import BUILDING_TOPIC, ALERTS_TOPIC

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

num_partitions = 2
replication_factor = 1

topic_names = [BUILDING_TOPIC, ALERTS_TOPIC]

topics_to_create = [
    NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )
    for topic_name in topic_names
]

try:
    admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
    print("Topics created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

print("My topics:")
for topic in admin_client.list_topics():
    if topic in topic_names:
        print(topic)

admin_client.close()