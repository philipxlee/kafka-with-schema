from config.kafka_config import KafkaConfig
from confluent_kafka import Producer


class KafkaProducer:

    def __init__(self):
        self.config = KafkaConfig().get_kafka_config()
        self.producer = Producer(self.config)

    def send_message(self, topic, message):
        self.producer.send(topic, message)
        self.producer.flush()

