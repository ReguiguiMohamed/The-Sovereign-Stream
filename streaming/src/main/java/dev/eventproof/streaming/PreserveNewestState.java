package dev.eventproof.streaming;

import java.time.Instant;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Emits only events that advance the materialized state for one entity. */
public final class PreserveNewestState
        extends KeyedProcessFunction<String, OperationalStateEvent, OperationalStateEvent> {
    private transient ValueState<String> latestEventId;
    private transient ValueState<Long> latestEventTime;

    @Override
    public void open(OpenContext ignored) {
        latestEventId = getRuntimeContext().getState(
                new ValueStateDescriptor<>("latest-event-id", String.class));
        latestEventTime = getRuntimeContext().getState(
                new ValueStateDescriptor<>("latest-event-time", Long.class));
    }

    @Override
    public void processElement(
            OperationalStateEvent event,
            Context ignored,
            Collector<OperationalStateEvent> output) throws Exception {
        if (event.getEventId().equals(latestEventId.value())) {
            return;
        }

        long candidateTime = Instant.parse(event.getEventTime()).toEpochMilli();
        Long currentTime = latestEventTime.value();
        if (currentTime == null || candidateTime > currentTime) {
            latestEventId.update(event.getEventId());
            latestEventTime.update(candidateTime);
            output.collect(event);
        }
    }
}
