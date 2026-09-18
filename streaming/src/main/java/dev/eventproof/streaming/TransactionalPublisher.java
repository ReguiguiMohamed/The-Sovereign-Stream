package dev.eventproof.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Publishes Jetstream messages to partition 0 of the record topic, one Kafka
 * transaction per batch.
 *
 * <p>A batch commits only after every send in it succeeded, so the resume cursor
 * never passes an unpublished message. Malformed messages go to the quarantine
 * topic in the same transaction.
 */
final class TransactionalPublisher {
    static final int MAX_BATCH = 2_000;
    private static final long SCAN_BACK = 2L * MAX_BATCH;
    private static final ObjectMapper JSON = new ObjectMapper();

    record Received(String message, Instant at) {}

    final AtomicLong published = new AtomicLong();
    final AtomicLong skipped = new AtomicLong();
    final AtomicLong quarantined = new AtomicLong();

    private final Producer<String, String> producer;
    private final String topic;
    private final String quarantineTopic;

    TransactionalPublisher(Producer<String, String> producer, String topic, String quarantineTopic) {
        this.producer = producer;
        this.topic = topic;
        this.quarantineTopic = quarantineTopic;
    }

    /**
     * Publishes one batch atomically and returns the new cursor: the highest
     * source sequence in the batch, or {@code cursor} when none is readable. Any
     * failure propagates before the commit; the caller must then stop.
     */
    long publish(List<Received> batch, long cursor) throws Exception {
        producer.beginTransaction();
        List<Future<RecordMetadata>> sends = new ArrayList<>();
        long last = cursor;
        long records = 0;
        long identities = 0;
        long rejected = 0;
        for (Received received : batch) {
            RecordStateEvent event;
            try {
                JsonNode message = JSON.readTree(received.message());
                // A readable sequence is safe to pass: the message is either
                // published or quarantined in this transaction.
                last = Math.max(last, JetstreamMapping.sequence(message.path("payload")));
                event = JetstreamMapping.toRecordState(message, received.at());
            } catch (Exception invalid) {
                ProducerRecord<String, String> record = new ProducerRecord<>(quarantineTopic, 0,
                        JetstreamMapping.sha256(received.message()), received.message());
                record.headers().add("reason",
                        String.valueOf(invalid.getMessage()).getBytes(StandardCharsets.UTF_8));
                sends.add(producer.send(record));
                rejected++;
                continue;
            }
            if (event == null) {
                identities++;
                continue;
            }
            sends.add(producer.send(new ProducerRecord<>(
                    topic, 0, CurrentStateRow.rowKey(event), EventJson.write(event))));
            records++;
        }
        producer.flush();
        // An earlier failed send stops the commit even if later sends succeeded.
        for (Future<RecordMetadata> send : sends) {
            send.get();
        }
        producer.commitTransaction();
        published.addAndGet(records);
        skipped.addAndGet(identities);
        quarantined.addAndGet(rejected);
        return last;
    }

    /**
     * The highest committed source_seq in the record topic, or -1 when it holds
     * none. The consumer must use read_committed, so aborted records are skipped.
     */
    static long lastCommittedSeq(Consumer<String, String> consumer, String topic)
            throws Exception {
        TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(List.of(partition));
        long begin = consumer.beginningOffsets(List.of(partition)).get(partition);
        long end = consumer.endOffsets(List.of(partition)).get(partition);
        for (long back = SCAN_BACK; ; back *= 2) {
            long start = Math.max(begin, end - back);
            consumer.seek(partition, start);
            long last = -1;
            int emptyPolls = 0;
            while (consumer.position(partition) < end) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                emptyPolls = records.isEmpty() ? emptyPolls + 1 : 0;
                if (emptyPolls == 3) {
                    throw new IllegalStateException("could not read " + topic + " up to " + end);
                }
                for (ConsumerRecord<String, String> record : records) {
                    if (record.offset() < end) {
                        last = Math.max(last, EventJson.read(record.value()).sourceSeq);
                    }
                }
            }
            // A window holding only aborted records says nothing: look further back.
            if (last >= 0 || start == begin) {
                return last;
            }
        }
    }
}
