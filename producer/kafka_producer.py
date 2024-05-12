from config.kafka_config import KafkaConfig
from confluent_kafka import Producer

"""
The KafkaProducer class is responsible for sending messages to a Kafka topic.
It creates a new producer instance and sends a message to the specified topic.
"""


class KafkaProducer:

    def __init__(self):
        """Initializes the KafkaProducer object."""
        self.config = KafkaConfig().get_kafka_config()
        self.producer = Producer(self.config)

    def send_message(self, topic, message):
        """Sends a message to the specified Kafka topic."""
        self.producer.send(topic, message)
        self.producer.flush()
