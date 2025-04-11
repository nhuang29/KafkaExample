from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'loan-events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='loan-processor-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Kafka Consumer is listening...")

for message in consumer:
    data = message.value
    print(f"Received: {data}")
    # You can now process the message (e.g., save to DB, trigger logic)