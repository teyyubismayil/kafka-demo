package org.teyyub.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerApplication {

    private final static String BOOTSTRAP_SERVERS_CONFIG = "127.0.0.1:9092";
    private final static String GROUP_ID_CONFIG = "console-consumer";
    private final static String AUTO_OFFSET_RESET_CONFIG = "earliest";
    private final static String TOPIC = "demo";

    public static void main(String[] args) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_CONFIG);

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        executorService.submit(() -> {
            try (var consumer = new KafkaConsumer<Long, String>(properties)) {
                consumer.subscribe(List.of(TOPIC));

                while (!Thread.currentThread().isInterrupted()) {
                    var records = consumer.poll(Duration.ofMillis(100));

                    for (var record : records) {
                        System.out.printf("Message received: %s\n", record.value());
                    }
                }
            }
        });

        System.out.println("Press enter to exit");

        var scanner = new Scanner(System.in);

        scanner.nextLine();

        executorService.shutdownNow();

        System.out.println("Exiting...");
    }
}
