package kafka.avro.performance.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class GenericRecordConsumerTest {
    @Test
    public void testMaxNumberOfRetries() {
        GenericRecordConsumer consumer = new GenericRecordConsumer();
        assertEquals(consumer.getNumberOfRecordsReceived(), 0, "No records should have been received");
    }
}
