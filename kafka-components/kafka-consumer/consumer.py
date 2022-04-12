from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
import generator
from utils import helper
import time
import json


def deserializer(data):
    output = data.decode("utf-8")
    return output


class Consumer:
    def __init__(self, bootstrap_server, topic):
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        print(self.topic)

    def consume(self):
        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_server,
                                 auto_offset_reset="earliest",
                                 enable_auto_commit=True,
                                 group_id = "simple-consumer",
                                 value_deserializer=deserializer,
                                 key_deserializer=deserializer)
        consumer.subscribe(topics=[self.topic])
        while True:
            for message in consumer:
                print("=================New Message==================")
                print(message.topic)
                print(message.key)
                print(message.value)
                time.sleep(1)

        # consumer.close()


def main():
    config = helper.read_config("config/kafka.cfg")
    BOOTSTRAP_SERVER = config["DEV"]["BOOTSTRAP_SERVER"]
    TOPIC = config["DEV"]["TOPIC"]

    consumer = Consumer(BOOTSTRAP_SERVER, TOPIC)
    consumer.consume()


if __name__ == "__main__":
    main()
