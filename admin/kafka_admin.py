from config.kafka_config import KafkaConfig
from confluent_kafka.admin import AdminClient, NewTopic


class KafkaAdmin:
    """
    KafkaAdmin class is used to manage the Kafka cluster.
    It provides methods to create, delete, and describe topics, as well as
    to list the available topics in the cluster.
    """

    BOOTSTRAP_SERVERS_KEY: str = "bootstrap.servers"

    def __init__(self) -> None:
        """Initializes the KafkaAdmin object."""
        self._config = KafkaConfig().get_kafka_config()
        self._admin = AdminClient(self._config)

    def topic_exists(self, topic_name) -> bool:
        """
        Checks if the specified topic exists in the Kafka cluster.
        :param topic_name: The name of the topic to check.
        :return: True if the topic exists, False otherwise.
        """
        all_topics = self._admin.list_topics()
        return topic_name in all_topics.topics.keys()

    def create_topic(self, topic_name, partitions=1, replication_factor=1) -> None:
        """
        Creates a new topic in the Kafka cluster.
        :param topic_name: The name of the topic to create.
        :param partitions: The number of partitions for the topic.
        :param replication_factor: The replication factor for the topic.
        """
        if not self.topic_exists(topic_name):
            new_topic = NewTopic(topic_name, partitions, replication_factor)
            self._admin.create_topics([new_topic])





