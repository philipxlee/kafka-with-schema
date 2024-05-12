import logging

"""
This class is responsible for loading the Kafka configuration from the client.properties file.
It reads the configuration from the file and returns it as a dictionary.
"""


class KafkaConfig:

    KAFKA_CLIENT_PATH = "../resources/client.properties"

    def __init__(self):
        """Initializes the KafkaConfig object."""
        self.logger = logging.getLogger(__name__)

    def get_kafka_config(self) -> dict:
        """Returns the Kafka configuration as a dictionary."""
        config = {}
        with open(self.KAFKA_CLIENT_PATH) as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split("=", 1)
                    config[parameter] = value.strip()
        self.logger.info("Kafka configuration loaded successfully.")
        return config
