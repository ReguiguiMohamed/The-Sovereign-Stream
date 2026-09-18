package dev.eventproof.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Maps one Bluesky Jetstream v2 message to record-state.v1.
 *
 * <p>Record bodies are dropped; only identity, order, lifecycle and the engagement
 * subject are kept. Account and sync events become account-level markers.
 */
final class JetstreamMapping {
    static final String SOURCE = "bluesky.jetstream.v2";
    static final List<String> COLLECTIONS =
            List.of("app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.feed.repost");
    static final String ACCOUNT = "account";
    static final String SYNC = "sync";
    static final Pattern DID = Pattern.compile("did:[a-z]+:[a-zA-Z0-9._:%-]{1,2048}");
    static final Pattern RECORD_KEY = Pattern.compile("[a-zA-Z0-9._:~-]{1,512}");

    private JetstreamMapping() {}

    /**
     * Returns null for identity events, which change no record. Throws
     * IllegalArgumentException for anything malformed or unknown.
     */
    static RecordStateEvent toRecordState(JsonNode message, Instant receivedAt) {
        JsonNode payload = message.path("payload");
        String type = text(message, "$type");
        if (!"message".equals(type)) {
            throw new IllegalArgumentException("unexpected envelope " + type);
        }
        String kind = text(payload, "$type");
        kind = kind.substring(kind.indexOf('#') + 1);
        RecordStateEvent event = new RecordStateEvent();
        event.schemaVersion = RecordStateEvent.SCHEMA_VERSION;
        event.source = SOURCE;
        event.sourceSeq = sequence(payload);
        event.eventTime = Instant.parse(text(payload, "time")).toString();
        event.receivedAt = receivedAt.toString();
        String did = matching(payload, "did", DID);
        switch (kind) {
            case "commit" -> {
                event.entityType = text(payload, "collection");
                event.entityId = did + "/" + matching(payload, "rkey", RECORD_KEY);
                event.eventType = text(payload, "operation");
                event.revision = text(payload, "rev");
                event.state = "delete".equals(event.eventType) ? "deleted" : "active";
                JsonNode subject = payload.path("record").path("subject").path("uri");
                event.subject = subject.isTextual() ? subject.asText() : null;
            }
            case ACCOUNT -> {
                event.entityType = ACCOUNT;
                event.entityId = did;
                event.eventType = ACCOUNT;
                event.revision = CurrentStateRow.padded(event.sourceSeq);
                // The status is nested; the sequence and time stay at the payload level.
                JsonNode account = payload.path(ACCOUNT);
                JsonNode active = account.path("active");
                if (!active.isBoolean()) {
                    throw new IllegalArgumentException("account event without active flag");
                }
                event.state = active.booleanValue() ? "active" : text(account, "status");
            }
            case SYNC -> {
                event.entityType = SYNC;
                event.entityId = did;
                event.eventType = SYNC;
                event.revision = CurrentStateRow.padded(event.sourceSeq);
                event.state = "resynced";
            }
            case "identity" -> {
                return null;
            }
            default -> throw new IllegalArgumentException("unknown event kind " + kind);
        }
        // Identity of one revision of one entity: redelivery keeps it, the cursor does not.
        event.eventId = sha256(String.join("\n",
                event.entityType, event.entityId, event.revision, event.eventType));
        return RecordStateEvent.validate(event);
    }

    static long sequence(JsonNode payload) {
        JsonNode seq = payload.path("seq");
        if (!seq.isIntegralNumber() || !seq.canConvertToLong() || seq.longValue() < 0) {
            throw new IllegalArgumentException("seq must be a non-negative integer");
        }
        return seq.longValue();
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isTextual() || value.asText().isBlank()) {
            throw new IllegalArgumentException(field + " must be a non-empty string");
        }
        return value.asText();
    }

    private static String matching(JsonNode node, String field, Pattern pattern) {
        String value = text(node, field);
        if (!pattern.matcher(value).matches()) {
            throw new IllegalArgumentException(field + " is malformed");
        }
        return value;
    }

    static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
