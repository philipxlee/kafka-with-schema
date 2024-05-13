class KafkaConsumer:

    def __init__(self, topic, group_id):
        self.topic = topic
        self.group_id = group_id
        self.consumer = KafkaConsumer(topic, group_id)

    def consume(self):
        for message in self.consumer:
            print(message.value)
