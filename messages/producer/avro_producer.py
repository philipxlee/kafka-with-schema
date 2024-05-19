from messages.producer.abstract_producer import AbstractProducer
from config.schema_registry import KafkaSchemaRegistry
from typing import override
import logging

logging.basicConfig(level=logging.INFO)


class AvroProducer(AbstractProducer):
    """
    The KafkaProducer class is responsible for sending messages to a Kafka topic.
    It creates a new producer instance and sends a message to the specified topic.
    """

    def __init__(self):
        """Initializes the AvroProducer object."""
        super().__init__()
        self._logger = logging.getLogger(__name__)
        self._schema_registry = KafkaSchemaRegistry()
        self._schema_registry_client = self._schema_registry.schema_client

    @override
    def configure_producer_schema(self, topic_name: str, schema: str, compatibility: str):
        """Creates a schema registry client."""
        self._schema_registry.register_schema(topic_name, schema)
        self._schema_registry.set_registry_compatability(topic_name, compatibility)
        self._logger.info("Schema registry client configured for producer.")

    @override
    def produce(self, topic, key, value):
        """Produces a message to the specified Kafka topic."""
        self.producer.produce(topic=topic, key=key, value=value)
        self._logger.info(f"Message sent to topic '{topic}'.")


