from confluent_kafka import Producer



def main():
    config = read_config()
    topic = "topic_0"

    # creates a new producer instance
    producer = Producer(config)

    # produces a sample message
    key = "key"
    value = "value"
    producer.produce(topic, key=key, value=value)
    print(f"Produced message to topic {topic}: key = {key:12} value = {value:12}")

    # send any outstanding or buffered messages to the Kafka broker
    producer.flush()


if __name__ == "__main__":
    main()
