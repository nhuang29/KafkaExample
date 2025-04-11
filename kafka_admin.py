from kafka.admin import KafkaAdminClient, NewTopic
import time

class KafkaManager:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='kafka-admin')


    def create_topics(self, topics):
        try:
            existing_topics = self.admin_client.list_topics()

            topics_to_delete = [topic for topic in existing_topics if not topic.startswith('__')]
            print(f"Topics to delete: {topics_to_delete}")
            if topics_to_delete:
                self.admin_client.delete_topics(topics=topics_to_delete)
            else:
                print("No non-internal topics found to delete.")

            time.sleep(20)

            print(f"Topics that still exist even after deletion: {self.admin_client.list_topics()}")

            new_topics = [NewTopic(name=topic['name'], num_partitions=topic['partitions'],
                                   replication_factor=topic['replication_factor']) for topic in topics]
            self.admin_client.create_topics(new_topics=new_topics, validate_only=False)
            print("Kafka topics created successfully.")
        except Exception as e:
            print("Error creating Kafka topics: {}".format(e))