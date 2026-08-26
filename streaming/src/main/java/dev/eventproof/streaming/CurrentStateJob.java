package dev.eventproof.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Minimal executable job for the current-state transition. */
public final class CurrentStateJob {
    private CurrentStateJob() {}

    public static DataStream<OperationalStateEvent> currentStateUpdates(
            DataStream<OperationalStateEvent> events) {
        return events
                .keyBy(OperationalStateEvent::getEntityId)
                .process(new PreserveNewestState());
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<OperationalStateEvent> events;
        if (args.length == 1) {
            FileSource<String> source = FileSource
                    .forRecordStreamFormat(new TextLineInputFormat(), new Path(args[0]))
                    .build();
            events = environment
                    .fromSource(source, WatermarkStrategy.noWatermarks(), "operational-state-ndjson")
                    .map(new ParseOperationalState());
        } else if (args.length == 0) {
            OperationalStateEvent newer = example(
                    "a".repeat(64), 0, "2026-08-25T00:00:00Z", "completed");
            OperationalStateEvent late = example(
                    "b".repeat(64), 1, "2026-08-24T23:58:00Z", "processing");
            events = environment.fromData(newer, newer, late);
        } else {
            throw new IllegalArgumentException("usage: CurrentStateJob [events.ndjson]");
        }
        currentStateUpdates(events).print();
        environment.execute("eventproof-current-state-demo");
    }

    private static OperationalStateEvent example(
            String eventId, int sequence, String eventTime, String state) {
        return new OperationalStateEvent(
                "operational-state.v1",
                eventId,
                "local-demo",
                sequence,
                "synthetic",
                "resource",
                "resource-0000",
                "state.updated",
                eventTime,
                "2026-08-25T00:00:01Z",
                state);
    }
}
