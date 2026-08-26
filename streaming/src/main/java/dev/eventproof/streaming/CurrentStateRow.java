package dev.eventproof.streaming;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

/**
 * The Bigtable current-state row contract, described in
 * docs/adr/0002-bigtable-current-state.md.
 *
 * <p>Writing and reading live together because they are one contract: the cell
 * the writer produces is the cell the reader must select.
 */
final class CurrentStateRow {
    static final String COLUMN_FAMILY = "cs";
    static final String EVENT_QUALIFIER = "event";

    private CurrentStateRow() {}

    /**
     * {@code entity_type} and {@code entity_id} are free-form, so a plain
     * delimiter join is not injective: ("a#b", "c") and ("a", "b#c") would share a
     * key. Escaping first leaves exactly one raw '#', which is the separator.
     */
    static String rowKey(String entityType, String entityId) {
        return escape(entityType) + "#" + escape(entityId);
    }

    private static String escape(String component) {
        return component.replace("%", "%25").replace("#", "%23");
    }

    static RowMutation mutation(String tableId, OperationalStateEvent event)
            throws JsonProcessingException {
        return RowMutation.create(TableId.of(tableId), rowKey(event.entityType, event.entityId))
                .setCell(
                        COLUMN_FAMILY,
                        EVENT_QUALIFIER,
                        cellTimestampMicros(event),
                        EventJson.write(event));
    }

    /**
     * The cell version is the event's own time, never arrival time: arrival time
     * would let a late delivery become the newest cell.
     */
    static long cellTimestampMicros(OperationalStateEvent event) {
        return Instant.parse(event.getEventTime()).toEpochMilli() * 1000L;
    }

    /** Returns the current event for one entity, or null when the row is absent. */
    static OperationalStateEvent read(
            BigtableDataClient client, String tableId, String entityType, String entityId)
            throws JsonProcessingException {
        Row row = client.readRow(TableId.of(tableId), rowKey(entityType, entityId));
        if (row == null) {
            return null;
        }
        List<RowCell> cells = row.getCells(COLUMN_FAMILY, EVENT_QUALIFIER);
        if (cells.isEmpty()) {
            return null;
        }
        // maxVersions(1) is collected asynchronously, so an older cell can still be
        // present. Pick the newest by timestamp rather than trusting collection.
        RowCell newest = cells.stream()
                .max(Comparator.comparingLong(RowCell::getTimestamp))
                .orElseThrow();
        return EventJson.read(newest.getValue().toStringUtf8());
    }
}
