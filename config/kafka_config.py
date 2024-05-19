import logging
import os
logging.basicConfig(level=logging.INFO)


class KafkaConfig:
    """
    This class is responsible for loading the Kafka configuration from the client.properties file.
    It reads the configuration from the file and returns it as a dictionary.
    """

    KAFKA_CLIENT_PATH = os.path.join(os.path.dirname(__file__), '../resources/client.properties')
    COMMENT_INDICATION = "#"
    EQUAL_SEPARATOR = "="
    SPLIT_LIMIT = 1

    def __init__(self):
        """Initializes the KafkaConfig object."""
        self.logger = logging.getLogger(__name__)

    def get_kafka_config(self) -> dict:
        """Returns the Kafka configuration as a dictionary."""
        config = {}
        with open(self.KAFKA_CLIENT_PATH) as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != self.COMMENT_INDICATION:
                    parameter, value = line.strip().split(
                        self.EQUAL_SEPARATOR, self.SPLIT_LIMIT
                    )
                    config[parameter] = value.strip()
        self.logger.info("Kafka configuration loaded successfully.")
        return config

