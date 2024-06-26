from confluent_kafka import Producer

from config.kafka_config import KafkaConfig
from abc import ABC, abstractmethod


class AbstractProducer(ABC):
    """
    The AbstractProducer class is an abstract class that defines the structure of a Kafka producer.
    It contains abstract produce method that must be implemented by the concrete producer classes,
    allowing subclasses to specify the type of schema or serialization format to use.
    """

    def __init__(self, topic_name: str) -> None:
        """Initializes the KafkaProducer object."""
        config_object = KafkaConfig()
        self._config = config_object.get_kafka_config()
        self._schema_topic_name = topic_name
        self._producer = Producer(self._config)

    @abstractmethod
    def configure_producer_schema(
        self, topic_name: str, schema: str, compatibility_level: str
    ):
        """Creates a schema registry client."""
        pass

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

    def commit(self):
        """Commits the messages produced by the producer."""
        self._producer.flush()

    @abstractmethod
    def _configure_serializers(self):
        """Configures the serializers for the producer."""
        pass
