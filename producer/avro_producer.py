from producer.abstract_producer import AbstractProducer
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

    @override
    def produce(self, topic, key, value):
        """Produces a message to the specified Kafka topic."""
        self.producer.produce(topic=topic, key=key, value=value)
        self._logger.info(f"Message sent to topic '{topic}'.")

