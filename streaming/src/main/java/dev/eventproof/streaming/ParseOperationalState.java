package dev.eventproof.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import java.time.Instant;
import java.util.stream.Stream;
import org.apache.flink.api.common.functions.MapFunction;

/** Parses one operational-state.v1 JSON object from an NDJSON input line. */
public final class ParseOperationalState
        implements MapFunction<String, OperationalStateEvent> {
    private static final ObjectMapper JSON = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    @Override
    public OperationalStateEvent map(String line) throws Exception {
        OperationalStateEvent event = JSON.readValue(line, OperationalStateEvent.class);
        if (!"operational-state.v1".equals(event.schemaVersion)) {
            throw new IllegalArgumentException("schema_version must be operational-state.v1");
        }
        if (Stream.of(
                        event.eventId,
                        event.runId,
                        event.source,
                        event.entityType,
                        event.entityId,
                        event.eventType,
                        event.eventTime,
                        event.receivedAt,
                        event.state)
                .anyMatch(value -> value == null || value.isBlank())) {
            throw new IllegalArgumentException("required contract fields must not be empty");
        }
        if (!event.eventId.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("event_id must be a lowercase SHA-256 digest");
        }
        if (event.runId.length() > 80 || event.sequence < 0) {
            throw new IllegalArgumentException("run_id or sequence is outside the contract");
        }
        Instant.parse(event.eventTime);
        Instant.parse(event.receivedAt);
        return event;
    }
}
