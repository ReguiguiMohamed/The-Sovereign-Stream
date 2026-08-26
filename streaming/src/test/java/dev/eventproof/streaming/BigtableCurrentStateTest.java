package dev.eventproof.streaming;

import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * EP-014, against the official bundled Bigtable emulator on a dynamic port.
 *
 * <p>The emulator is in-memory, so these tests prove the row contract, the client
 * behaviour and late-write materialization semantics. They prove nothing about
 * durability across a restart.
 */
public class BigtableCurrentStateTest {
    private static final String PROJECT = "eventproof-local";
    private static final String INSTANCE = "eventproof-local";
    private static final String TABLE = "current-state";
    private static final String ENTITY_TYPE = "resource";
    private static final String ENTITY_ID = "resource-0000";

    @ClassRule
    public static final MiniClusterWithClientResource FLINK =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    /** Starts on a free port before each test and is stopped by the rule after it. */
    @Rule
    public final BigtableEmulatorRule emulator = BigtableEmulatorRule.create();

    private BigtableDataClient data;

    @Before
    public void createCurrentStateTable() throws Exception {
        try (BigtableTableAdminClient admin = BigtableTableAdminClient.create(
                BigtableTableAdminSettings.newBuilderForEmulator(emulator.getPort())
                        .setProjectId(PROJECT)
                        .setInstanceId(INSTANCE)
                        .build())) {
            admin.createTable(CreateTableRequest.of(TABLE)
                    .addFamily(CurrentStateRow.COLUMN_FAMILY, GCRULES.maxVersions(1)));
        }
        data = BigtableDataClient.create(
                BigtableDataSettings.newBuilderForEmulator(emulator.getPort())
                        .setProjectId(PROJECT)
                        .setInstanceId(INSTANCE)
                        .build());
    }

    @After
    public void closeDataClient() {
        if (data != null) {
            data.close();
        }
    }

    /**
     * The storage rule alone, with no Flink filter in front of it: a mutation
     * carrying an older event_time cannot become the visible current value even
     * when it is written last.
     */
    @Test
    public void aLaterMutationWithAnOlderEventTimeDoesNotBecomeCurrent() throws Exception {
        OperationalStateEvent newer =
                event("a".repeat(64), 0, "2026-08-25T00:00:00Z", "completed");
        OperationalStateEvent late =
                event("b".repeat(64), 1, "2026-08-24T23:58:00Z", "processing");

        data.mutateRow(CurrentStateRow.mutation(TABLE, newer));
        data.mutateRow(CurrentStateRow.mutation(TABLE, late));

        OperationalStateEvent current =
                CurrentStateRow.read(data, TABLE, ENTITY_TYPE, ENTITY_ID);

        assertNotNull(current);
        assertEquals(newer.getEventId(), current.getEventId());
        assertEquals("completed", current.getState());
        assertEquals("2026-08-25T00:00:00Z", current.getEventTime());
    }

    /** The whole path: MiniCluster job -> production sink -> emulator row. */
    @Test
    public void flinkSinkMaterializesOneCurrentRowPerEntity() throws Exception {
        OperationalStateEvent newer =
                event("a".repeat(64), 0, "2026-08-25T00:00:00Z", "completed");
        OperationalStateEvent late =
                event("b".repeat(64), 1, "2026-08-24T23:58:00Z", "processing");

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        CurrentStateJob
                .currentStateUpdates(environment.fromData(newer, newer, late))
                .sinkTo(BigtableCurrentStateSink
                        .forEmulator(PROJECT, INSTANCE, TABLE, emulator.getPort()));
        // Bounded source: execute() returns when the job is finished, so the row is
        // readable without polling or sleeping.
        environment.execute("bigtable-current-state-test");

        OperationalStateEvent current =
                CurrentStateRow.read(data, TABLE, ENTITY_TYPE, ENTITY_ID);

        assertNotNull(current);
        assertEquals(newer.getEventId(), current.getEventId());
        assertEquals("completed", current.getState());
        assertEquals(1, rowCount());
    }

    /**
     * Bigtable creates tables with MILLIS granularity and rejects a cell timestamp
     * that is not a whole millisecond, so the cell version resolves to exactly one
     * millisecond. This locks both halves of that: sub-millisecond detail is lost,
     * and one millisecond of difference still orders two events.
     */
    @Test
    public void cellVersionIsAWholeMillisecondAndOrdersAtThatResolution() throws Exception {
        OperationalStateEvent newer =
                event("a".repeat(64), 0, "2026-08-25T00:00:00.002000Z", "completed");
        OperationalStateEvent sameMillisecond =
                event("b".repeat(64), 1, "2026-08-25T00:00:00.002999Z", "processing");
        OperationalStateEvent olderMillisecond =
                event("c".repeat(64), 2, "2026-08-25T00:00:00.001000Z", "processing");

        // The known limit: 999 microseconds of difference collapse onto one cell.
        assertEquals(
                CurrentStateRow.cellTimestampMicros(newer),
                CurrentStateRow.cellTimestampMicros(sameMillisecond));
        // One millisecond of difference survives, and the value stays millisecond
        // aligned so Bigtable accepts it.
        assertEquals(
                1_000L,
                CurrentStateRow.cellTimestampMicros(newer)
                        - CurrentStateRow.cellTimestampMicros(olderMillisecond));
        assertEquals(0L, CurrentStateRow.cellTimestampMicros(newer) % 1_000L);

        data.mutateRow(CurrentStateRow.mutation(TABLE, newer));
        data.mutateRow(CurrentStateRow.mutation(TABLE, olderMillisecond));

        OperationalStateEvent current =
                CurrentStateRow.read(data, TABLE, ENTITY_TYPE, ENTITY_ID);

        assertNotNull(current);
        assertEquals(newer.getEventId(), current.getEventId());
        assertEquals("completed", current.getState());
    }

    @Test
    public void rowKeySeparatorCannotCollideAcrossEntityTypeAndId() {
        assertEquals(
                "resource#resource-0000",
                CurrentStateRow.rowKey("resource", "resource-0000"));
        // Without escaping both of these would be "a#b#c".
        org.junit.Assert.assertNotEquals(
                CurrentStateRow.rowKey("a#b", "c"), CurrentStateRow.rowKey("a", "b#c"));
    }

    private int rowCount() {
        int rows = 0;
        for (Row ignored : data.readRows(Query.create(TableId.of(TABLE)))) {
            rows++;
        }
        return rows;
    }

    private static OperationalStateEvent event(
            String eventId, int sequence, String eventTime, String state) {
        return new OperationalStateEvent(
                "operational-state.v1",
                eventId,
                "bigtable-test",
                sequence,
                "synthetic",
                ENTITY_TYPE,
                ENTITY_ID,
                "state.updated",
                eventTime,
                "2026-08-25T00:00:01Z",
                state);
    }
}
