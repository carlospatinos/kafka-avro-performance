package kafka.avro.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import kafka.avro.performance.consumer.GenericRecordConsumer;
import kafka.avro.performance.producer.GenericRecordProducer;

public class KafkaTest {
    @ParameterizedTest(name = "{index} - Value {0} is an odd number.")
    @ValueSource(ints = { 1, 3, 5, -3, 15, Integer.MAX_VALUE }) // six numbers
    void isOdd_ShouldReturnTrueForOddNumbers(int number) {
        assertTrue(GenericRecordProducer.isOdd(number));
    }

    @ParameterizedTest(name = "{index} - Value {0} is an odd number.")
    @ValueSource(ints = { 1 }) // six numbers
    @Disabled("Disabled until coordinating individual threads in Consumer and Producer!")
    void isKafkaConsumerGettingAllKafkaProducedNumbers(int number) {
        GenericRecordProducer genericRecordProducer = new GenericRecordProducer();
        genericRecordProducer.setNumberOfRecordsToSend(number);
        genericRecordProducer.writeMessage();

        GenericRecordConsumer genericRecordConsumer = new GenericRecordConsumer();
        genericRecordConsumer.readMessages();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // assertEquals(
        // genericRecordProducer.getNumberOfRecordsSent(),
        // genericRecordConsumer.getNumberOfRecordsReceived());
    }
}
