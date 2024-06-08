import os
import json
import threading

from src.logger import Logger
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError


class Kafka:
    def __init__(self):
        logger = Logger(show=True)
        self.log = logger.get_logger(__name__)

        # bootstrap_server = os.getenv('KAFKA_CFG_ADVERTISED_LISTENERS').split('//')[-1]
        bootstrap_server = 'kafka:9092'
        self.bootstrap_servers = [bootstrap_server]
        
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        self.log.info('Connected to Kafka.')

        # создание топика
        self.topic_name = 'predictions'
        try:
            self.admin_client.create_topics(
                new_topics=[NewTopic(name=self.topic_name, num_partitions=1, replication_factor=1)],
            )
            self.log.info(f'Topic {self.topic_name} created.')
        except TopicAlreadyExistsError as _:
            self.log.info(f'Topic {self.topic_name} already exists.')

        # подписчик
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=json.loads,
            auto_offset_reset="latest",
        )
        self.consumer.subscribe(self.topic_name)
        self.log.info(f'Consumer created and subscribed to {self.topic_name} topic.')

        # издатель
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.log.info(f'Producer created.')

    def send(self, data: dict):
        self.producer.send(self.topic_name, data)
        self.producer.flush()
        self.log.info(f"Sent data: , {self.data}")

    # https://stackoverflow.com/questions/53263393/is-there-a-python-api-for-event-driven-kafka-consumer
    def register_kafka_listener(self, listener):
        def poll():
            self.consumer.poll(timeout_ms=6000)
            for msg in self.consumer:
                self.log.info(f"Entered the loop\nKey: {msg.key} Value: {msg.value}")
                listener(msg)

        self.log.info(f"About to register listener to topic: , {self.topic_name}")
        t1 = threading.Thread(target=poll)
        t1.start()
        self.log.info('started a background thread')
