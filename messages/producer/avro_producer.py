from messages.producer.abstract_producer import AbstractProducer
from config.schema_registry import KafkaSchemaRegistry
from typing import override
from messages.data.user import User
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import KafkaException
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)


import logging

logging.basicConfig(level=logging.INFO)


class AvroProducer(AbstractProducer):
    """
    The KafkaProducer class is responsible for sending messages to a Kafka topic.
    It creates a new producer instance and sends a message to the specified topic.
    """

    def __init__(self, topic_name: str) -> None:
        """Initializes the AvroProducer object."""
        super().__init__(topic_name)
        self._logger = logging.getLogger(__name__)
        self._schema_registry = KafkaSchemaRegistry(topic_name)
        self._schema_registry_client = self._schema_registry.schema_client
        self._avro_serializer = None
        self._string_serializer = None

    @override
    def configure_producer_schema(
        self, topic_name: str, schema: str, compatibility: str
    ) -> None:
        """Creates a schema registry client."""
        self._schema_registry.register_schema(topic_name, schema)
        self._schema_registry.set_registry_compatability(topic_name, compatibility)
        self._configure_serializers()
        self._logger.info("Schema registry client configured for producer.")

    @override
    def produce(self, topic, key, value=None) -> None:
        """Produces a message to the specified Kafka topic."""
        try:
            byte_value = (
                self._avro_serializer(
                    value, SerializationContext(topic, MessageField.VALUE)
                )
                if value is not None
                else None
            )
            self.producer.produce(
                topic=topic, key=self._string_serializer(key), value=byte_value
            )
            self._logger.info(f"Message sent to topic '{topic}'.")
        except KafkaException as e:
            self._logger.error(f"Error producing message: {e}")

    @override
    def _configure_serializers(self):
        """Configures the serializers for the producer."""
        schema_str = self._schema_registry.get_schema_str()
        self._avro_serializer = AvroSerializer(self._schema_registry_client, schema_str)
        self._string_serializer = StringSerializer("utf-8")

