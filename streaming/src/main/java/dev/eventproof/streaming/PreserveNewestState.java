package dev.eventproof.streaming;

import java.time.Duration;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Emits an event only when its revision is newer than the last one emitted for
 * the same record.
 *
 * <p>Revisions are AT Protocol TIDs, which sort lexically in commit order, so the
 * result does not depend on arrival order and a redelivered revision is dropped.
 */
public final class PreserveNewestState
        extends KeyedProcessFunction<String, RecordStateEvent, RecordStateEvent> {
    private static final Duration STATE_TTL = Duration.ofDays(3);

    private transient ValueState<String> latestRevision;

    @Override
    public void open(OpenContext ignored) {
        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("latest-revision", String.class);
        // Bounded by topic retention; an older revision replayed after expiry is
        // still kept from becoming current by the revision-ordered Bigtable cell.
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(STATE_TTL).build());
        latestRevision = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
            RecordStateEvent event, Context ignored, Collector<RecordStateEvent> output)
            throws Exception {
        String current = latestRevision.value();
        if (current == null || event.revision.compareTo(current) > 0) {
            latestRevision.update(event.revision);
            output.collect(event);
        }
    }
}
