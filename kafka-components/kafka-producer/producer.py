from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
import generator
from utils import helper
import time
import json


def serializer(data):
    output = data.encode("utf-8")
    return output


class Producer:
    def __init__(self, bootstrap_server, topic):
        self.bootstrap_server = bootstrap_server
        self.topic = topic

    def produce(self):
        producer = KafkaProducer(bootstrap_servers=self.bootstrap_server,
                                 key_serializer=serializer,
                                 value_serializer=serializer,
                                 acks="all",
                                 retries=3)
        while True:
            key = 0
            customer_pre_check = generator.Generator()
            data = customer_pre_check.generate_data()
            producer.send(self.topic, value=data, key=str(key))
            key += 1
            time.sleep(1)
        producer.close()


def main():
    config = helper.read_config("config/kafka.cfg")
    BOOTSTRAP_SERVER = config["DEV"]["BOOTSTRAP_SERVER"]
    TOPIC = config["DEV"]["TOPIC"]
    while True:
        try:
            admin = KafkaAdminClient()
            topic = NewTopic(name=TOPIC,
                             num_partitions=1,
                             replication_factor=1)
            admin.create_topics([topic])
            break
        except Exception as e:
            print(e)
            print("Oops! Perhaps the topic has already been created")
            admin.delete_topics([TOPIC])

    producer = Producer(BOOTSTRAP_SERVER, TOPIC)
    producer.produce()


if __name__ == "__main__":
    main()
