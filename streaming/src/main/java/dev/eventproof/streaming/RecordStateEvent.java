package dev.eventproof.streaming;

import static dev.eventproof.streaming.JetstreamMapping.ACCOUNT;
import static dev.eventproof.streaming.JetstreamMapping.COLLECTIONS;
import static dev.eventproof.streaming.JetstreamMapping.DID;
import static dev.eventproof.streaming.JetstreamMapping.RECORD_KEY;
import static dev.eventproof.streaming.JetstreamMapping.SYNC;

import java.io.Serializable;
import java.time.Instant;
import java.util.Set;
import java.util.stream.Stream;

/** Typed mirror of contracts/record-state.v1.schema.json, with its validation. */
public class RecordStateEvent implements Serializable {
    static final String SCHEMA_VERSION = "record-state.v1";
    private static final String TID = "[2-7a-j][2-7a-z]{12}";

    public String schemaVersion;
    public String eventId;
    public String source;
    // Boxed so a missing value arrives as null and is rejected, not read as 0.
    public Long sourceSeq;
    public String entityType;
    public String entityId;
    public String eventType;
    public String revision;
    public String eventTime;
    public String receivedAt;
    public String state;
    public String subject;

    static RecordStateEvent validate(RecordStateEvent event) {
        require(SCHEMA_VERSION.equals(event.schemaVersion),
                "schema_version must be " + SCHEMA_VERSION);
        require(Stream.of(event.eventId, event.source, event.entityType, event.entityId,
                        event.eventType, event.revision, event.eventTime, event.receivedAt,
                        event.state)
                .noneMatch(value -> value == null || value.isBlank()),
                "required contract fields must not be empty");
        require(event.eventId.matches("[0-9a-f]{64}"),
                "event_id must be a lowercase SHA-256 digest");
        require(event.sourceSeq != null && event.sourceSeq >= 0,
                "source_seq must be a non-negative integer");
        if (COLLECTIONS.contains(event.entityType)) {
            String[] id = event.entityId.split("/", -1);
            require(id.length == 2 && DID.matcher(id[0]).matches()
                            && RECORD_KEY.matcher(id[1]).matches(),
                    "record entity_id must be did/rkey");
            require(Set.of("create", "update", "delete").contains(event.eventType),
                    "event_type must be create, update or delete");
            require(("delete".equals(event.eventType) ? "deleted" : "active")
                            .equals(event.state),
                    "state must follow event_type");
            require(event.revision.matches(TID), "revision must be an AT Protocol TID");
        } else {
            require(ACCOUNT.equals(event.entityType) || SYNC.equals(event.entityType),
                    "entity_type must be a subscribed collection, account or sync");
            require(event.entityType.equals(event.eventType),
                    "event_type must equal entity_type for account markers");
            require(DID.matcher(event.entityId).matches(), "entity_id must be a DID");
            require(event.revision.equals(CurrentStateRow.padded(event.sourceSeq)),
                    "revision must be the zero-padded source_seq");
            require(!SYNC.equals(event.entityType) || "resynced".equals(event.state),
                    "sync state must be resynced");
            require(event.subject == null, "account markers have no subject");
        }
        Instant.parse(event.eventTime);
        Instant.parse(event.receivedAt);
        return event;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
