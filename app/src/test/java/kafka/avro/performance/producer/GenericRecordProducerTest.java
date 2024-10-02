package kafka.avro.performance.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class GenericRecordProducerTest {
    @Test
    public void testMaxNumberOfRetries() {
        GenericRecordProducer producer = new GenericRecordProducer();
        assertEquals(producer.getNumberOfRecordsSent(), 0, "No records should have been received");
    }
}
