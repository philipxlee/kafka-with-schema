from confluent_kafka.schema_registry import SchemaRegistryClient
import configparser

"""
This class is responsible for loading the Schema Registry configuration from a configuration file.
It reads the configuration from the file and initializes the Schema Registry client.
"""


class KafkaSchemaRegistry:

    SCHEMA_CONFIG_API_PATH = "../resources/schema_config.ini"
    SCHEMA_CONFIG_KEY = "SchemaRegistry"
    SCHEMA_CONFIG_URL = "url"

    def __init__(self):
        """Initializes the KafkaSchemaRegistry object."""
        self._config = configparser.ConfigParser()
        self._schema_registry = None
        self._schema_client = None
        self._initialize_schema_registry()

    def _initialize_schema_registry(self) -> None:
        """Initializes the Schema Registry client."""
        self._config.read(self.SCHEMA_CONFIG_API_PATH)
        self._schema_registry = self._config.get(self.SCHEMA_CONFIG_KEY, self.SCHEMA_CONFIG_URL)
        self._schema_client = SchemaRegistryClient({"url": self._schema_registry})

    @property
    def schema_client(self) -> SchemaRegistryClient:
        """Gets the Schema Registry client."""
        return self._schema_client


