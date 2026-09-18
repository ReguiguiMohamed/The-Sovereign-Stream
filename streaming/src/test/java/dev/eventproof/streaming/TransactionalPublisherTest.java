package dev.eventproof.streaming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import dev.eventproof.streaming.TransactionalPublisher.Received;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

public class TransactionalPublisherTest {
    private static final String TOPIC = "records";
    private static final String QUARANTINE = "quarantine";
    private static final Instant AT = Instant.parse("2026-09-17T16:34:45Z");

    @Test
    public void anEarlierFailedSendBlocksTheCommitEvenIfLaterSendsSucceed() {
        MockProducer<String, String> producer = new MockProducer<>(
                true, null, new StringSerializer(), new StringSerializer()) {
            private boolean failed;

            @Override
            public synchronized Future<RecordMetadata> send(
                    ProducerRecord<String, String> record, Callback callback) {
                if (!failed) {
                    failed = true;
                    return CompletableFuture.failedFuture(new KafkaException("broker rejected"));
                }
                return super.send(record, callback);
            }
        };
        producer.initTransactions();

        assertThrows(ExecutionException.class, () -> new TransactionalPublisher(
                producer, TOPIC, QUARANTINE).publish(batch(commit(100), commit(101)), 50));
        assertFalse(producer.transactionCommitted());
        assertEquals(1, producer.uncommittedRecords().size());
    }

    @Test
    public void aFailedCommitLeavesTheCursorWhereItWas() throws Exception {
        MockProducer<String, String> producer = producer();
        producer.commitTransactionException = new KafkaException("coordinator unavailable");
        TransactionalPublisher publisher = new TransactionalPublisher(producer, TOPIC, QUARANTINE);

        assertThrows(KafkaException.class, () -> publisher.publish(batch(commit(100)), 50));
        assertFalse(producer.transactionCommitted());
        assertEquals(0, publisher.published.get());
    }

    @Test
    public void malformedMessagesAreQuarantinedInTheSameTransaction() throws Exception {
        MockProducer<String, String> producer = producer();
        TransactionalPublisher publisher = new TransactionalPublisher(producer, TOPIC, QUARANTINE);

        long cursor = publisher.publish(batch(
                commit(100),
                commit(101).replace("\"operation\":\"create\"", "\"operation\":\"upsert\""),
                "not json",
                identity(102)), 50);

        assertTrue(producer.transactionCommitted());
        assertEquals(102, cursor);
        List<ProducerRecord<String, String>> history = producer.history();
        assertEquals(3, history.size());
        assertEquals(TOPIC, history.get(0).topic());
        assertEquals(QUARANTINE, history.get(1).topic());
        assertEquals("not json", history.get(2).value());
        assertEquals(1, publisher.published.get());
        assertEquals(2, publisher.quarantined.get());
        assertEquals(1, publisher.skipped.get());
    }

    @Test
    public void restartResumesFromTheHighestCommittedSequence() throws Exception {
        MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        consumer.assign(List.of(partition));
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        consumer.updateEndOffsets(Map.of(partition, 2L));
        consumer.addRecord(record(0, 205));
        consumer.addRecord(record(1, 204));

        assertEquals(205, TransactionalPublisher.lastCommittedSeq(consumer, TOPIC));
    }

    @Test
    public void anEmptyTopicStartsLive() throws Exception {
        MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        consumer.updateEndOffsets(Map.of(partition, 0L));

        assertEquals(-1, TransactionalPublisher.lastCommittedSeq(consumer, TOPIC));
        assertEquals(-1, JetstreamProducer.url(-1, 0).indexOf("cursor="));
        assertTrue(JetstreamProducer.url(205, 0).endsWith("&cursor=205"));
        // A refused instance is not retried: the next attempt is another one.
        assertNotEquals(JetstreamProducer.url(205, 0), JetstreamProducer.url(205, 1));
        assertEquals(JetstreamProducer.url(205, 0), JetstreamProducer.url(205, 2));
    }

    private static MockProducer<String, String> producer() {
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        producer.initTransactions();
        return producer;
    }

    private static List<Received> batch(String... messages) {
        return Arrays.stream(messages).map(m -> new Received(m, AT)).toList();
    }

    private static String commit(long seq) {
        return "{\"$type\":\"message\",\"payload\":{\"$type\":"
                + "\"network.bsky.jetstream.subscribeEvents#commit\","
                + "\"collection\":\"app.bsky.feed.post\",\"did\":\"" + Records.DID + "\","
                + "\"operation\":\"create\",\"rev\":\"3mvq3ibybj72k\",\"rkey\":\"r" + seq + "\","
                + "\"seq\":" + seq + ",\"time\":\"2026-09-17T16:34:44.687783Z\"}}";
    }

    private static String identity(long seq) {
        return "{\"$type\":\"message\",\"payload\":{\"$type\":"
                + "\"network.bsky.jetstream.subscribeEvents#identity\","
                + "\"did\":\"" + Records.DID + "\",\"seq\":" + seq
                + ",\"time\":\"2026-09-17T16:34:44.687783Z\"}}";
    }

    private static ConsumerRecord<String, String> record(long offset, long seq) throws Exception {
        return new ConsumerRecord<>(TOPIC, 0, offset, "key", EventJson.write(
                Records.record(Records.POST, Records.POST_ID, Records.OLDER, "create", seq)));
    }
}
