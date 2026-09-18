package dev.eventproof.streaming;

/** Test fixtures for record-state.v1. */
final class Records {
    static final String POST = "app.bsky.feed.post";
    static final String DID = "did:plc:rrd35f6zsrmm7szkrlzd42ug";
    static final String POST_ID = DID + "/3mvq3hy4fsl2l";
    // Real revisions from a Jetstream probe, in commit order. SAME_MS_NEWER differs
    // from OLDER only in the clock-id bits, so both fall in one millisecond.
    static final String OLDER = "3mvq3hy4jpl2l";
    static final String SAME_MS_NEWER = "3mvq3hy4jpl2m";
    static final String NEWER = "3mvq3hz4jpl2l";
    static final long SEQ = 25984168574L;

    private Records() {}

    static RecordStateEvent record(
            String entityType, String entityId, String revision, String operation, long seq) {
        RecordStateEvent event = new RecordStateEvent();
        event.schemaVersion = "record-state.v1";
        event.eventId = JetstreamMapping.sha256(entityType + entityId + revision + operation);
        event.source = "bluesky.jetstream.v2";
        event.sourceSeq = seq;
        event.entityType = entityType;
        event.entityId = entityId;
        event.eventType = operation;
        event.revision = revision;
        event.eventTime = "2026-09-17T16:34:34.287599Z";
        event.receivedAt = "2026-09-17T16:34:34.300000Z";
        event.state = "delete".equals(operation) ? "deleted" : "active";
        return event;
    }

    static RecordStateEvent record(
            String entityType, String entityId, String revision, String operation) {
        return record(entityType, entityId, revision, operation, SEQ);
    }

    static RecordStateEvent post(String revision, String operation) {
        return record(POST, POST_ID, revision, operation);
    }

    static RecordStateEvent account(String did, long seq, String state) {
        RecordStateEvent event = record(
                JetstreamMapping.ACCOUNT, did, CurrentStateRow.padded(seq),
                JetstreamMapping.ACCOUNT, seq);
        event.state = state;
        return event;
    }

    static RecordStateEvent sync(String did, long seq) {
        RecordStateEvent event = record(
                JetstreamMapping.SYNC, did, CurrentStateRow.padded(seq),
                JetstreamMapping.SYNC, seq);
        event.state = "resynced";
        return event;
    }
}
