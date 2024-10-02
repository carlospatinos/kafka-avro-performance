package kafka.avro.performance.producer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class GenericRecordProducer {
    private Properties properties;
    private int recordsToSend = 100;
    private AtomicInteger sentMessages;

    public static void main(String[] args) {
        GenericRecordProducer genericRecordProducer = new GenericRecordProducer();
        genericRecordProducer.setNumberOfRecordsToSend(500);
        genericRecordProducer.writeMessage();
    }

    public GenericRecordProducer() {
        // create kafka producer
        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        sentMessages = new AtomicInteger();
    }

    public void setNumberOfRecordsToSend(int number) {
        this.recordsToSend = number;
    }

    public int getNumberOfRecordsSent() {
        return sentMessages.get();
    }

    public void writeMessage() {

        Producer<String, GenericRecord> producer = new KafkaProducer<>(properties);

        // avro schema
        String messageSchema = "{\"type\":\"record\",\"name\":\"DataMessage\",\"namespace\":\"kafka.avro.performance\",\"fields\":[{\"name\":\"content\",\"type\":\"string\",\"doc\":\"Message content\"},{\"name\":\"date_time\",\"type\":\"string\",\"doc\":\"Datetime when the message was generated\"}]}";

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(messageSchema);

        // prepare the kafka record
        for (int i = 0; i < recordsToSend; i++) {
            GenericRecord avroRecord = new GenericData.Record(schema);
            avroRecord.put("content", "DataMessage is alive " + i);
            avroRecord.put("date_time", Instant.now().toString());
            // System.out.println(avroRecord);
            ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("avro-topic-1", null, avroRecord);
            producer.send(record);
            sentMessages.incrementAndGet();
            // ensures record is sent before closing the producer
            producer.flush();
        }
        producer.close();
        System.out.println("sentMessages: " + sentMessages.get());
    }

    public static boolean isOdd(int number) {
        return number % 2 != 0;
    }
}
