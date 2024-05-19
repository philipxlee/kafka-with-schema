import concurrent

from config.kafka_config import KafkaConfig
from confluent_kafka.admin import AdminClient, NewTopic
import logging

logging.basicConfig(level=logging.INFO)


class KafkaAdmin:
    """
    KafkaAdmin class is used to manage the Kafka cluster.
    It provides methods to create, delete, and describe topics, as well as
    to list the available topics in the cluster.
    """

    BOOTSTRAP_SERVERS_KEY: str = "bootstrap.servers"

    def __init__(self) -> None:
        """Initializes the KafkaAdmin object."""
        self._kafka_client = KafkaConfig()
        self._config = self._kafka_client.get_kafka_config()
        self._admin = AdminClient(self._config)
        self._logger = logging.getLogger(__name__)

    def topic_exists(self, topic_name: str) -> bool:
        """
        Checks if the specified topic exists in the Kafka cluster.
        :param topic_name: The name of the topic to check.
        :return: True if the topic exists, False otherwise.
        """
        all_topics = self._admin.list_topics()
        return topic_name in all_topics.topics.keys()

    def create_topic(self, topic_name: str, partitions=1) -> None:
        """
        Creates a new topic in the Kafka cluster.
        :param topic_name: The name of the topic to create.
        :param partitions: The number of partitions for the topic.
        """
        if not self.topic_exists(topic_name):
            new_topic = NewTopic(topic_name, partitions)
            res = self._admin.create_topics([new_topic])
            concurrent.futures.wait(
                res.values()
            )  # Important for create_topics thread to complete
            self._logger.info(f"Topic '{topic_name}' created successfully.")
        else:
            self._logger.warning(f"Topic '{topic_name}' already exists.")

    def delete_topic(self, topic_name: str) -> None:
        """
        Deletes a topic from the Kafka cluster.
        :param topic_name: The name of the topic to delete.
        """
        if self.topic_exists(topic_name):
            res = self._admin.delete_topics([topic_name])
            concurrent.futures.wait(res.values())
            self._logger.info(f"Topic '{topic_name}' deleted successfully.")
        else:
            self._logger.warning(f"Topic '{topic_name}' does not exist.")
