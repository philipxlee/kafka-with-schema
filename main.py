from admin.kafka_admin import KafkaAdmin
from messages.producer.avro_producer import AvroProducer
from messages.data.user import User

import os


def main():
    admin = KafkaAdmin()
    admin.create_topic("topic_1")
    avro_producer = AvroProducer("topic_1")
    try:
        with open(os.path.join(os.path.dirname(__file__), "resources/schemas/user_schema.asvc"), "r") as file:
            schema_str = file.read()
    except FileNotFoundError as e:
        print(f"Error reading schema file: {e}")
        exit(1)

    avro_producer.configure_producer_schema("topic_1", schema_str, "BACKWARD")
    user_data_dict = {
        "name": "John Doe",
        "user_id": 12345,
        "first_name": "John",
        "last_name": "Doe",
        "age": 30,
        "email": "philip@gmail.com"
    }
    user = User(user_data_dict)
    user_dict = user.user_to_dictionary()
    avro_producer.produce("topic_1", user_dict["user_id"], user_dict)
    avro_producer.commit()


if __name__ == "__main__":
    main()
