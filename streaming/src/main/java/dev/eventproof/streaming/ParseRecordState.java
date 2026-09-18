package dev.eventproof.streaming;

import org.apache.flink.api.common.functions.MapFunction;

/** Parses one record-state.v1 JSON line for the Flink job. */
public final class ParseRecordState implements MapFunction<String, RecordStateEvent> {
    @Override
    public RecordStateEvent map(String line) throws Exception {
        return RecordStateEvent.validate(EventJson.read(line));
    }
}
