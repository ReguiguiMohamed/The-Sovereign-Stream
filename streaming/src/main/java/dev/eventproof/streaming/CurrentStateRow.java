package dev.eventproof.streaming;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * The Bigtable current-state row contract, described in streaming/README.md.
 *
 * <p>Every write is a check-and-mutate: the server compares the stored order key
 * with the new one and applies the mutation atomically only when the new one is
 * greater, so order holds under batching, retries, replay and expired Flink state.
 */
final class CurrentStateRow {
    static final String FAMILY = "cs";
    static final String EVENT = "event";
    static final String REVISION = "revision";
    static final String SEQ = "seq";
    static final String PURGE = "purge";
    /** Recent-activity index: one row per event, newest first, expired by age. */
    static final String ACTIVITY = "act";
    static final String KEY = "k";
    private static final String ACTIVITY_PREFIX = "~act#";
    // '~' sorts after every entity type, so the index never collides with state.
    private static final int ACTIVITY_DIGITS = 14;
    private static final long HORIZON = 99_999_999_999_999L;
    /** Matches the act family's max-age rule; reads apply it too, because GC is lazy. */
    static final Duration ACTIVITY_WINDOW = Duration.ofHours(1);
    // Bigtable cell timestamps are microseconds, at millisecond granularity.
    private static final long MICROS = 1000;
    // One version per cell: a later successful write replaces it.
    private static final long VERSION = 0;

    private CurrentStateRow() {}

    /** Escaping leaves exactly one raw '#'; Flink state uses the same key. */
    static String rowKey(String entityType, String entityId) {
        return escape(entityType) + "#" + escape(entityId);
    }

    static String rowKey(RecordStateEvent event) {
        return rowKey(event.entityType, event.entityId);
    }

    static String accountKey(String did) {
        return rowKey(JetstreamMapping.ACCOUNT, did);
    }

    private static String escape(String component) {
        return component.replace("%", "%25").replace("#", "%23");
    }

    /** Reverses {@link #rowKey}: entity type, then entity id. */
    static String[] parseRowKey(String key) {
        int hash = key.indexOf('#');
        return new String[] {unescape(key.substring(0, hash)), unescape(key.substring(hash + 1))};
    }

    private static String unescape(String component) {
        return component.replace("%23", "#").replace("%25", "%");
    }

    /** Fixed width, so byte order is numeric order. */
    static String padded(long seq) {
        return String.format("%020d", seq);
    }

    /**
     * Index key for one event: the complement of its receive time, so a plain
     * prefix read returns the most recent events first without a table scan.
     */
    static String activityKey(RecordStateEvent event) {
        return ACTIVITY_PREFIX
                + String.format("%0" + ACTIVITY_DIGITS + "d", HORIZON - receivedMillis(event))
                + "#" + event.eventId.substring(0, 12);
    }

    private static long receivedMillis(RecordStateEvent event) {
        return Instant.parse(event.receivedAt).toEpochMilli();
    }

    static Instant activityTime(String activityKey) {
        int start = ACTIVITY_PREFIX.length();
        return Instant.ofEpochMilli(HORIZON
                - Long.parseLong(activityKey.substring(start, start + ACTIVITY_DIGITS)));
    }

    /**
     * Records which row the event touched; the row itself stays authoritative. The
     * cell carries the event's own receive time, which is what the family's max-age
     * rule measures, and makes a retried write replace its cell rather than add one.
     */
    static RowMutation activity(String tableId, RecordStateEvent event) {
        return RowMutation.create(TableId.of(tableId), activityKey(event))
                .setCell(ACTIVITY, KEY, receivedMillis(event) * MICROS, rowKey(event));
    }

    /**
     * The newest {@code limit} index rows received within {@link #ACTIVITY_WINDOW} of
     * {@code now}. Garbage collection is asynchronous, so the window is a read filter
     * as well as a retention rule; a row whose cell falls outside it is not returned.
     */
    static Query recent(String tableId, int limit, Instant now) {
        long cutoff = now.minus(ACTIVITY_WINDOW).toEpochMilli() * MICROS;
        return Query.create(TableId.of(tableId))
                .prefix(ACTIVITY_PREFIX)
                .filter(FILTERS.chain()
                        .filter(FILTERS.family().exactMatch(ACTIVITY))
                        .filter(FILTERS.qualifier().exactMatch(KEY))
                        .filter(FILTERS.timestamp().range().startClosed(cutoff))
                        .filter(FILTERS.limit().cellsPerColumn(1)))
                .limit(limit);
    }

    static boolean purges(RecordStateEvent event) {
        return JetstreamMapping.SYNC.equals(event.eventType)
                || (JetstreamMapping.ACCOUNT.equals(event.eventType)
                        && "deleted".equals(event.state));
    }

    static BigtableDataSettings settings(
            String projectId, String instanceId, String emulatorHost) {
        BigtableDataSettings.Builder builder;
        if (emulatorHost == null) {
            builder = BigtableDataSettings.newBuilder();
        } else {
            int colon = emulatorHost.lastIndexOf(':');
            builder = BigtableDataSettings.newBuilderForEmulator(
                    emulatorHost.substring(0, colon),
                    Integer.parseInt(emulatorHost.substring(colon + 1)));
        }
        return builder.setProjectId(projectId).setInstanceId(instanceId).build();
    }

    /** Stores the event unless the row already holds this or a later revision. */
    static ConditionalRowMutation write(String tableId, RecordStateEvent event)
            throws JsonProcessingException {
        return ConditionalRowMutation.create(TableId.of(tableId), rowKey(event))
                .condition(atLeast(REVISION, event.revision))
                .otherwise(Mutation.create()
                        .setCell(FAMILY, EVENT, VERSION, EventJson.write(event))
                        .setCell(FAMILY, REVISION, VERSION, event.revision)
                        .setCell(FAMILY, SEQ, VERSION, padded(event.sourceSeq)));
    }

    /** Raises the account's purge boundary to this event's sequence. */
    static ConditionalRowMutation purgeMarker(String tableId, RecordStateEvent event) {
        String seq = padded(event.sourceSeq);
        return ConditionalRowMutation.create(TableId.of(tableId), accountKey(event.entityId))
                .condition(atLeast(PURGE, seq))
                .otherwise(Mutation.create().setCell(FAMILY, PURGE, VERSION, seq));
    }

    /**
     * Deletes the account's records whose sequence is at or below the purge
     * event's. Later records, written after the marker, stay.
     */
    static void purgeRecords(BigtableDataClient client, String tableId, RecordStateEvent event) {
        String seq = padded(event.sourceSeq);
        for (String collection : JetstreamMapping.COLLECTIONS) {
            Query rows = Query.create(TableId.of(tableId))
                    .prefix(rowKey(collection, event.entityId + "/"))
                    .filter(FILTERS.chain()
                            .filter(column(SEQ))
                            .filter(FILTERS.value().strip()));
            for (Row row : client.readRows(rows)) {
                client.checkAndMutateRow(
                        ConditionalRowMutation.create(TableId.of(tableId), row.getKey())
                                .condition(FILTERS.chain()
                                        .filter(column(SEQ))
                                        .filter(FILTERS.value().range().endClosed(seq)))
                                .then(Mutation.create().deleteRow()));
            }
        }
    }

    /**
     * Returns the stored event for one entity, or null when it is absent or its
     * account hides it: an inactive account, or a purge at or after the record.
     * At most two rows are read.
     */
    static RecordStateEvent read(
            BigtableDataClient client, String tableId, String entityType, String entityId)
            throws IOException {
        String key = rowKey(entityType, entityId);
        Query query = Query.create(TableId.of(tableId))
                .rowKey(key)
                .filter(FILTERS.chain()
                        .filter(FILTERS.family().exactMatch(FAMILY))
                        .filter(FILTERS.limit().cellsPerColumn(1)));
        String accountKey = null;
        if (JetstreamMapping.COLLECTIONS.contains(entityType)) {
            accountKey = accountKey(entityId.substring(0, Math.max(0, entityId.indexOf('/'))));
            query.rowKey(accountKey);
        }
        Map<ByteString, Map<String, String>> rows = new HashMap<>();
        for (Row row : client.readRows(query)) {
            Map<String, String> cells = new HashMap<>();
            for (RowCell cell : row.getCells()) {
                cells.put(cell.getQualifier().toStringUtf8(), cell.getValue().toStringUtf8());
            }
            rows.put(row.getKey(), cells);
        }
        Map<String, String> record = rows.get(ByteString.copyFromUtf8(key));
        if (record == null || !record.containsKey(EVENT)) {
            return null;
        }
        Map<String, String> account = accountKey == null
                ? Map.of() : rows.getOrDefault(ByteString.copyFromUtf8(accountKey), Map.of());
        if (account.containsKey(EVENT)
                && !"active".equals(EventJson.read(account.get(EVENT)).state)) {
            return null;
        }
        if (account.containsKey(PURGE) && record.get(SEQ).compareTo(account.get(PURGE)) <= 0) {
            return null;
        }
        return EventJson.read(record.get(EVENT));
    }

    private static Filter column(String qualifier) {
        return column(FAMILY, qualifier);
    }

    private static Filter column(String family, String qualifier) {
        return FILTERS.chain()
                .filter(FILTERS.family().exactMatch(family))
                .filter(FILTERS.qualifier().exactMatch(qualifier))
                .filter(FILTERS.limit().cellsPerColumn(1));
    }

    private static Filter atLeast(String qualifier, String value) {
        return FILTERS.chain()
                .filter(column(qualifier))
                .filter(FILTERS.value().range().startClosed(value));
    }
}
