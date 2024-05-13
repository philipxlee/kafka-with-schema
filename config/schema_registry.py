from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError
import configparser
import logging

logging.basicConfig(level=logging.INFO)


class KafkaSchemaRegistry:
    """
    This class is responsible for loading the Schema Registry configuration from a configuration file.
    It reads the configuration from the file and initializes the Schema Registry client.
    """

    SCHEMA_CONFIG_API_PATH: str = "../resources/schema_config.ini"
    SCHEMA_PATH: str = "../resources/schema.ini"
    SCHEMA_HEADING: str = "Schema"
    SCHEMA_KEY: str = "key"
    SCHEMA_SECRET: str = "secret"
    SCHEMA_CONFIG_HEADING: str = "SchemaRegistry"
    SCHEMA_CONFIG_KEY: str = "url"
    SCHEMA_TYPE: str = "AVRO"

    def __init__(self) -> None:
        """Initializes the KafkaSchemaRegistry object."""
        self._config = configparser.ConfigParser()
        self._schema_client = None
        self._schema = ""
        self.logger = logging.getLogger(__name__)
        self._configure_schema_registry()

    def register_schema(self, subject: str, schema: str) -> int:
        """Registers the Schema with the Schema Registry."""
        try:
            schema_object = Schema(schema, self.SCHEMA_TYPE)
            schema_id = self._schema_client.register_schema(subject, schema_object)
            self.logger.info("Schema successfully registered.")
            return schema_id
        except SchemaRegistryError as e:
            self.logger.error(f"Error registering schema: {e}")
            exit(1)

    def set_registry_compatability(
        self, subject: str, compatibility_level: str
    ) -> None:
        """Sets the compatibility level for the Schema Registry."""
        try:
            self._schema_client.set_compatibility(subject, compatibility_level)
            self.logger.info("Compatibility level updated successfully.")
        except SchemaRegistryError as e:
            self.logger.error(f"Error setting compatibility level: {e}")
            exit(1)

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
        try:
            with open(schema_path, "r") as schema_file:
                self._schema = schema_file.read()
        except FileNotFoundError as e:
            self.logger.error(f"Error reading schema file: {e}")
            exit(1)

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
