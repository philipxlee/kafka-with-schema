from confluent_kafka.schema_registry import SchemaRegistryClient
import configparser

"""
This class is responsible for loading the Schema Registry configuration from a configuration file.
It reads the configuration from the file and initializes the Schema Registry client.
"""


class KafkaSchemaRegistry:

    SCHEMA_CONFIG_API_PATH = "../resources/schema_config.ini"
    SCHEMA_PATH = "../resources/schema.ini"
    SCHEMA_HEADING = "Schema"
    SCHEMA_KEY = "avro_schema_v1"
    SCHEMA_CONFIG_HEADING = "SchemaRegistry"
    SCHEMA_CONFIG_KEY = "url"

    def __init__(self) -> None:
        """Initializes the KafkaSchemaRegistry object."""
        self._config = configparser.ConfigParser()
        self._schema_client = None
        self._schema = ""
        self._configure_schema_registry()

    @property
    def schema_client(self) -> SchemaRegistryClient:
        """Gets the Schema Registry client."""
        return self._schema_client

    @property
    def schema(self) -> str:
        """Gets the Schema."""
        return self._schema

    @schema.setter
    def schema(self, schema_path: str, schema_heading: str, schema_key: str) -> None:
        """Sets the Schema from a specified file."""
        self._config.read(schema_path)
        self._schema = self._config.get(schema_heading, schema_key)

    def _configure_schema_registry(self) -> None:
        """Initializes the Schema Registry client."""
        self._config.read(self.SCHEMA_CONFIG_API_PATH)
        schema_registry = self._config.get(self.SCHEMA_CONFIG_HEADING, self.SCHEMA_CONFIG_KEY)
        self._schema_client = SchemaRegistryClient({"url": schema_registry})

