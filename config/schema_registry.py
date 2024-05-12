from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
import configparser
import logging
logging.basicConfig(level=logging.INFO)

"""
This class is responsible for loading the Schema Registry configuration from a configuration file.
It reads the configuration from the file and initializes the Schema Registry client.
"""


class KafkaSchemaRegistry:

    SCHEMA_CONFIG_API_PATH = "../resources/schema_config.ini"
    SCHEMA_PATH = "../resources/schema.ini"
    SCHEMA_HEADING = "Schema"
    SCHEMA_KEY = "key"
    SCHEMA_SECRET = "secret"
    SCHEMA_CONFIG_HEADING = "SchemaRegistry"
    SCHEMA_CONFIG_KEY = "url"
    SCHEMA_TYPE = "AVRO"

    def __init__(self) -> None:
        """Initializes the KafkaSchemaRegistry object."""
        self._config = configparser.ConfigParser()
        self._schema_client = None
        self._schema = ""
        self.logger = logging.getLogger(__name__)
        self._configure_schema_registry()

    def register_schema(self, subject: str, schema: str) -> int:
        """Registers the Schema with the Schema Registry."""
        schema_object = Schema(schema, self.SCHEMA_TYPE)
        schema_id = self._schema_client.register_schema(subject, schema_object)
        self.logger.info("Schema successfully registered.")
        return schema_id

    @property
    def schema_client(self) -> SchemaRegistryClient:
        """Gets the Schema Registry client."""
        return self._schema_client

    @property
    def schema(self) -> str:
        """Gets the Schema."""
        return self._schema

    @schema.setter
    def schema(self, schema_path: str) -> None:
        """Sets the Schema from a specified file."""
        with open(schema_path, "r") as schema_file:
            self._schema = schema_file.read()

    def _configure_schema_registry(self) -> None:
        """Initializes the Schema Registry client."""
        self._config.read(self.SCHEMA_CONFIG_API_PATH)
        schema_key = self._config.get(self.SCHEMA_HEADING, self.SCHEMA_KEY)
        schema_secret = self._config.get(self.SCHEMA_HEADING, self.SCHEMA_SECRET)
        schema_registry = self._config.get(self.SCHEMA_CONFIG_HEADING, self.SCHEMA_CONFIG_KEY)
        self._schema_client = SchemaRegistryClient(
            {
                "url": schema_registry,
                "basic.auth.user.info": schema_key + ":" + schema_secret,
            }
        )
        self.logger.info("Schema Registry client initialized successfully.")

