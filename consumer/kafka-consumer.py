import os
from kafka import KafkaConsumer
from json import loads

topic = os.getenv("KAFKA_TOPIC", "instap")
group_id = os.getenv("GROUP_ID", "my-group")
consumer = KafkaConsumer(
     topic,
     bootstrap_servers=['broker:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     api_version=(0, 10, 1),
     group_id=group_id,
     value_deserializer=lambda x: loads(x.decode('utf-8')))
print("consumer in waiting..")
for message in consumer:
    message = message.value
    print('{} read'.format(message))