package dev.eventproof.streaming;

import java.io.Serializable;

/** Typed mirror of contracts/operational-state.v1.schema.json. */
public class OperationalStateEvent implements Serializable {
    public String schemaVersion;
    public String eventId;
    public String runId;
    // Boxed so a missing or null sequence arrives as null instead of defaulting
    // to a valid-looking 0. ParseOperationalState rejects it.
    public Integer sequence;
    public String source;
    public String entityType;
    public String entityId;
    public String eventType;
    public String eventTime;
    public String receivedAt;
    public String state;

    public OperationalStateEvent() {}

    public OperationalStateEvent(
            String schemaVersion,
            String eventId,
            String runId,
            Integer sequence,
            String source,
            String entityType,
            String entityId,
            String eventType,
            String eventTime,
            String receivedAt,
            String state) {
        this.schemaVersion = schemaVersion;
        this.eventId = eventId;
        this.runId = runId;
        this.sequence = sequence;
        this.source = source;
        this.entityType = entityType;
        this.entityId = entityId;
        this.eventType = eventType;
        this.eventTime = eventTime;
        this.receivedAt = receivedAt;
        this.state = state;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEntityId() {
        return entityId;
    }

    public String getEventTime() {
        return eventTime;
    }

    public String getState() {
        return state;
    }

    @Override
    public String toString() {
        return entityId + "=" + state + "@" + eventTime;
    }
}
