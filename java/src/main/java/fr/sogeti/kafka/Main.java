package fr.sogeti.kafka;

import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;

public class Main {

    public static void main(String[] args) {

	final String ipServer = "10.226.159.191:9092";
	// Properties for the producer
	Properties propertiesProducer = new Properties();
	propertiesProducer.put("bootstrap.servers", ipServer);
	propertiesProducer.put("auto.create.topics.enable", true);
	propertiesProducer.put("acks", "all");
	propertiesProducer.put("retries", 0);
	propertiesProducer.put("batch.size", 16384);
	propertiesProducer.put("linger.ms", 1);
	propertiesProducer.put("buffer.memory", 33554432);
	propertiesProducer.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	propertiesProducer.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	// Properties for the consumer
	Properties propertiesConsumer = new Properties();
	propertiesConsumer.put("bootstrap.servers", ipServer);
	propertiesConsumer.put("group.id", "testtopic");
	propertiesConsumer.put("key.deserializer", StringDeserializer.class.getName());
	propertiesConsumer.put("value.deserializer", StringDeserializer.class.getName());
	// Clients creation
	ProducerKafka producerKafka = new ProducerKafka(propertiesProducer);
	ConsumerKafka consumerKafka = new ConsumerKafka(propertiesConsumer);
	// The producer publishes messages and then the consumer displays them
	consumerKafka.subscribe("testtopic");
	producerKafka.sendMessage("testtopic", "Salut");
	producerKafka.sendMessage("testtopic", "ca va");
	consumerKafka.consumeAll();
	// Closing clients
	producerKafka.close();
	consumerKafka.close();

    }

}
