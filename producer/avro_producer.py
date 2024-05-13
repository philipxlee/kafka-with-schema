from producer.abstract_producer import AbstractProducer
from typing import override

"""
The KafkaProducer class is responsible for sending messages to a Kafka topic.
It creates a new producer instance and sends a message to the specified topic.
"""


class AvroProducer(AbstractProducer):

    def __init__(self):
        """Initializes the AvroProducer object."""
        super().__init__()

    @override
    def produce(self, topic, key, value):
        """Produces a message to the specified Kafka topic."""
        self._producer.produce(topic=topic, key=key, value=value)
        self._producer.flush()
        print(f"Message sent to topic {topic} successfully.")






