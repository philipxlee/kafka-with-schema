from confluent_kafka import Producer
from config.kafka_config import KafkaConfig
from abc import ABC, abstractmethod


"""
The AbstractProducer class is an abstract class that defines the structure of a Kafka producer.
It contains abstract produce method that must be implemented by the concrete producer classes,
allowing subclasses to specify the type of schema or serialization format to use.
"""


class AbstractProducer(ABC):

    def __init__(self):
        """Initializes the KafkaProducer object."""
        self._config = KafkaConfig().get_kafka_config()
        self._producer = Producer(self.config)

    @abstractmethod
    def produce(self, topic, key, value):
        """Produces a message to the specified Kafka topic."""
        pass

    @property
    def config(self):
        """Gets the Kafka configuration."""
        return self._config

    @property
    def producer(self):
        """Gets the Kafka producer."""
        return self._producer
