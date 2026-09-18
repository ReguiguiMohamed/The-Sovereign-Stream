package dev.eventproof.streaming;

import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;
import static dev.eventproof.streaming.Records.DID;
import static dev.eventproof.streaming.Records.NEWER;
import static dev.eventproof.streaming.Records.OLDER;
import static dev.eventproof.streaming.Records.POST;
import static dev.eventproof.streaming.Records.SAME_MS_NEWER;
import static dev.eventproof.streaming.Records.post;
import static dev.eventproof.streaming.Records.record;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.threeten.bp.Duration;

/** Bigtable row contract tests against the official bundled emulator. */
public class BigtableCurrentStateTest {
    private static final String PROJECT = "eventproof-local";
    private static final String INSTANCE = "eventproof-local";
    private static final String TABLE = "current-state";

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
                    .addFamily(CurrentStateRow.FAMILY, GCRULES.maxVersions(1))
                    .addFamily(CurrentStateRow.ACTIVITY, GCRULES.maxAge(Duration.ofHours(1))));
        }
        data = BigtableDataClient.create(CurrentStateRow.settings(PROJECT, INSTANCE, host()));
    }

    @After
    public void closeDataClient() {
        if (data != null) {
            data.close();
        }
    }

    @Test
    public void sameMillisecondOlderRevisionWrittenLastIsIgnored() throws Exception {
        write(post(SAME_MS_NEWER, "delete"));
        write(post(OLDER, "create"));

        assertEquals(SAME_MS_NEWER, current().revision);
    }

    @Test
    public void staleCreateAfterDeleteIsIgnored() throws Exception {
        write(post(NEWER, "delete"));
        write(post(OLDER, "create"));
        write(post(NEWER, "delete"));

        assertEquals("deleted", current().state);
    }

    /** Concurrent, shuffled and repeated writes still leave the newest revision. */
    @Test
    public void concurrentAndRetriedWritesKeepTheNewest() throws Exception {
        List<String> revisions = new ArrayList<>();
        for (String revision : List.of(OLDER, SAME_MS_NEWER, NEWER)) {
            for (int copy = 0; copy < 20; copy++) {
                revisions.add(revision);
            }
        }
        Collections.shuffle(revisions, new Random(20260917));
        List<ApiFuture<Boolean>> writes = new ArrayList<>();
        for (String revision : revisions) {
            writes.add(data.checkAndMutateRowAsync(
                    CurrentStateRow.write(TABLE, post(revision, "create"))));
        }
        ApiFutures.allAsList(writes).get();

        assertEquals(NEWER, current().revision);
    }

    /** A second job with empty Flink state replays an older revision; storage keeps the newer. */
    @Test
    public void replayWithEmptyFlinkStateKeepsTheNewest() throws Exception {
        runJob(post(NEWER, "delete"));
        runJob(post(OLDER, "create"));

        assertEquals("deleted", current().state);
    }

    @Test
    public void accountDeletionPurgesEarlierRecordsAndHidesTheRest() throws Exception {
        RecordStateEvent before = record(POST, DID + "/before", OLDER, "create", 10);
        RecordStateEvent after = record(POST, DID + "/after", OLDER, "create", 30);
        try (BigtableCurrentStateSink.Writer writer = writer()) {
            writer.write(before, null);
            writer.write(after, null);
            writer.write(Records.account(DID, 20, "deleted"), null);
            writer.flush(false);
        }

        assertEquals(null, data.readRow(TableId.of(TABLE), CurrentStateRow.rowKey(before)));
        assertNotNull(data.readRow(TableId.of(TABLE), CurrentStateRow.rowKey(after)));
        assertNull(CurrentStateRow.read(data, TABLE, POST, after.entityId));
    }

    @Test
    public void syncPurgesEarlierRecordsOnlyAndAnOlderPurgeCannotLowerIt() throws Exception {
        RecordStateEvent before = record(POST, DID + "/before", OLDER, "create", 10);
        RecordStateEvent after = record(POST, DID + "/after", OLDER, "create", 30);
        try (BigtableCurrentStateSink.Writer writer = writer()) {
            writer.write(before, null);
            writer.write(after, null);
            writer.write(Records.sync(DID, 20), null);
            writer.write(Records.sync(DID, 5), null);
            writer.flush(false);
        }
        write(before);

        assertNull(CurrentStateRow.read(data, TABLE, POST, before.entityId));
        RecordStateEvent visible = CurrentStateRow.read(data, TABLE, POST, after.entityId);
        assertNotNull(visible);
        assertEquals(after.eventId, visible.eventId);
    }

    /** A rejected mutation fails the flush, so the checkpoint that needs it fails. */
    @Test
    public void aRejectedMutationFailsTheFlush() throws Exception {
        try (BigtableCurrentStateSink.Writer writer = new BigtableCurrentStateSink.Writer(
                BigtableDataClient.create(CurrentStateRow.settings(PROJECT, INSTANCE, host())),
                "missing-table")) {
            writer.write(post(OLDER, "create"), null);

            assertThrows(IOException.class, () -> writer.flush(false));
        }
    }

    private String host() {
        return "localhost:" + emulator.getPort();
    }

    private BigtableCurrentStateSink.Writer writer() throws IOException {
        return new BigtableCurrentStateSink.Writer(
                BigtableDataClient.create(CurrentStateRow.settings(PROJECT, INSTANCE, host())),
                TABLE);
    }

    private void write(RecordStateEvent event) throws Exception {
        data.checkAndMutateRow(CurrentStateRow.write(TABLE, event));
    }

    private RecordStateEvent current() throws Exception {
        return CurrentStateRow.read(data, TABLE, POST, Records.POST_ID);
    }

    private void runJob(RecordStateEvent... events) throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        CurrentStateJob
                .currentStateUpdates(environment.fromData(events))
                .sinkTo(new BigtableCurrentStateSink(PROJECT, INSTANCE, TABLE, host()));
        environment.execute("bigtable-current-state-test");
    }
}
