package dev.eventproof.streaming;

import static dev.eventproof.streaming.KafkaSettings.require;

import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Kafka record-state topic to Bigtable current state. Checkpointing and restart
 * behaviour come from the Flink cluster configuration.
 */
public final class CurrentStateJob {
    private CurrentStateJob() {}

    /** Stable operator uid, so checkpointed state restores into the upgraded job. */
    static final String STATE_UID = "preserve-newest-state";

    public static DataStream<RecordStateEvent> currentStateUpdates(
            DataStream<RecordStateEvent> events) {
        return events
                .keyBy(CurrentStateRow::rowKey)
                .process(new PreserveNewestState())
                .uid(STATE_UID)
                .name(STATE_UID);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        Properties kafka = KafkaSettings.fromEnvironment();
        // Only committed producer transactions are processed.
        kafka.setProperty("isolation.level", "read_committed");
        // Checkpointed offsets take precedence on restore; a fresh job replays the
        // retained topic, which the conditional writes make idempotent.
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setProperties(kafka)
                .setTopics(require("KAFKA_TOPIC"))
                .setGroupId("current-state")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<RecordStateEvent> events = environment
                .fromSource(source, WatermarkStrategy.noWatermarks(), "record-state-topic")
                .uid("record-state-topic")
                .map(new ParseRecordState())
                .uid("parse-record-state");
        currentStateUpdates(events)
                .sinkTo(new BigtableCurrentStateSink(
                        require("BIGTABLE_PROJECT_ID"),
                        require("BIGTABLE_INSTANCE_ID"),
                        require("BIGTABLE_TABLE_ID"),
                        System.getenv("BIGTABLE_EMULATOR_HOST")))
                .uid("bigtable-current-state");
        environment.execute("eventproof-current-state");
    }
}
