from kafka import KafkaConsumer
from kafka import KafkaProducer
import sys


brockerUrl = '10.226.159.191:9092'


class Producer:

    producer = None

    def __init__(self, brockerUrl):

        self.producer = KafkaProducer(bootstrap_servers=brockerUrl)
        print("Producer created")

    def sendMessage(self, topic, message):

        self.producer.send(topic, str.encode(message))
        print("Sent message " + message + " on topic " + topic)

    def close(self):

        self.producer.close


class Consumer:

    Consumer = None

    def __init__(self, brockerUrl):

        self.consumer = KafkaConsumer(
            bootstrap_servers=brockerUrl, auto_offset_reset='earliest')
        print("Producer created")

    def subscribe(self, topic):

        self.consumer.subscribe([topic])
        print("Subsribed to topic " + topic)

    def consumeAll(self):

        for message in self.consumer:
            print(message.value.decode())

    def close(self):

        self.consumer.close()


def main():

    try:

        print("Running")
        producer = Producer(brockerUrl)
        consumer = Consumer(brockerUrl)
        consumer.subscribe("topic")
        producer.sendMessage("topic", "Salut")
        consumer.consumeAll()
        producer.close()
        consumer.close()

    except KeyboardInterrupt:
        print("Shutdown requested...exiting")

    sys.exit(0)


if __name__ == '__main__':
    main()
