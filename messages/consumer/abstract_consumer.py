from config.kafka_config import KafkaConfig
from confluent_kafka import Consumer
from abc import ABC, abstractmethod

import logging

logging.basicConfig(level=logging.INFO)


class AbstractConsumer(ABC):
    """
    This class is an abstract class that defines the structure of a Kafka consumer.
    Subclasses must implement the consume method to specify the behavior of the consumer.
    """

    GROUP_ID = "group.id"

    def __init__(self, topic_name: str, group_id: str) -> None:
        """Initializes the KafkaConsumer object."""
        config_object = KafkaConfig()
        self._config = config_object.get_kafka_config()
        self._config[self.GROUP_ID] = group_id
        self._topic = topic_name
        self._group_id = group_id
        self._consumer = Consumer(self._config)
        self._logger = logging.getLogger(__name__)

    @abstractmethod
    def consume(self) -> None:
        """Consume messages from the specified Kafka topic."""
        self._consumer.subscribe([self._topic])
        self._logger.info(f"Consuming messages from topic: {self._topic}")
        try:
            while True:
                message = self._consumer.poll(timeout=1.0)
                if message is None:
                    continue
                if message.error():
                    self._logger.error(f"Consumer error: {message.error()}")
                    continue
                self._process_consumed_message(message)
        except KeyboardInterrupt:
            self._logger.info("Consumer stopped.")
        finally:
            self._consumer.close()

    def _process_consumed_message(self, message) -> None:
        self._logger.info(f"Consumed message: {message.value()}")
        byte_message = message.value()
        decoded_message = byte_message.decode("utf-8")
        self._logger.info(f"Decoded message: {decoded_message}")

