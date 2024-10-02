package kafka.avro.performance.consumer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;

import java.time.Duration;

public class GenericRecordConsumer {
    private Properties properties;

    private int maxNumberOfRetries = 2;
    private int sleepTimeInMillis = 50;

    private AtomicInteger receivedMessages;

    public static void main(String[] args) {
        GenericRecordConsumer genericRecordConsumer = new GenericRecordConsumer();
        genericRecordConsumer.readMessages();

    }

    public void setMaxNumberOfRetries(int maxNumber) {
        this.maxNumberOfRetries = maxNumber;
    }

    public int getNumberOfRecordsReceived() {
        return receivedMessages.get();
    }

    public GenericRecordConsumer() {
        // create kafka producer
        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "generic-record-consumer-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        receivedMessages = new AtomicInteger();
    }

    public void readMessages() {

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton("avro-topic-1"));

        // poll the record from the topic
        int retries = 0;
        while (retries < maxNumberOfRetries) {
            ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, GenericRecord> record : records) {
                System.out.println("Message content: " + record.value().get("content"));
                System.out.println("Message time: " + record.value().get("date_time"));
                receivedMessages.incrementAndGet();
            }
            consumer.commitAsync();

            try {
                Thread.sleep(sleepTimeInMillis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            retries++;
        }
        System.out.println("sentMessages: " + receivedMessages.get());
        consumer.close();
    }
}
