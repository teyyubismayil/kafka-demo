package org.teyyub.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class ProducerApplication {

    private final static String BOOTSTRAP_SERVERS_CONFIG = "127.0.0.1:9092";
    private final static String TOPIC = "demo";

    public static void main(String[] args) {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (var producer = new KafkaProducer<Long, String>(properties)) {
            System.out.println("Enter message or \"bye\" to exit");
            String message;
            var scanner = new Scanner(System.in);

            while(!(message = scanner.nextLine()).equalsIgnoreCase("bye")) {
                var record = new ProducerRecord<Long, String>(TOPIC, message);
                producer.send(record);
                producer.flush();

                System.out.printf("Message published: %s\n", message);
            }

            System.out.println("Exiting...");
        }
    }
}
