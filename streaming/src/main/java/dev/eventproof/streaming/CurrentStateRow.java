package dev.eventproof.streaming;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
import java.time.Instant;

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

    /** Narrows the point read to the newest version of the one cell we store. */
    private static final Filter NEWEST_EVENT_CELL = FILTERS.chain()
            .filter(FILTERS.family().exactMatch(COLUMN_FAMILY))
            .filter(FILTERS.qualifier().exactMatch(EVENT_QUALIFIER))
            .filter(FILTERS.limit().cellsPerColumn(1));

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
     *
     * <p>Bigtable expresses timestamps in microseconds but creates tables with
     * MILLIS granularity and rejects any value that is not a whole millisecond, so
     * the sub-millisecond part is dropped here rather than at write time. Two
     * events inside one millisecond therefore share a cell and cannot be ordered
     * by storage.
     */
    static long cellTimestampMicros(OperationalStateEvent event) {
        Instant eventTime = Instant.parse(event.getEventTime());
        return Math.addExact(
                Math.multiplyExact(eventTime.getEpochSecond(), 1_000_000L),
                (eventTime.getNano() / 1_000_000) * 1_000L);
    }

    /** Returns the current event for one entity, or null when the row is absent. */
    static OperationalStateEvent read(
            BigtableDataClient client, String tableId, String entityType, String entityId)
            throws JsonProcessingException {
        Row row = client.readRow(
                TableId.of(tableId), rowKey(entityType, entityId), NEWEST_EVENT_CELL);
        if (row == null || row.getCells().isEmpty()) {
            return null;
        }
        return EventJson.read(row.getCells().get(0).getValue().toStringUtf8());
    }
}
