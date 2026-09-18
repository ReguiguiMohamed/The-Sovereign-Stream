package dev.eventproof.streaming;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Restores the job's keyed state from a savepoint into a job with a different
 * source, then replays an older revision. Without restored state that revision
 * would be emitted.
 */
public class SavepointRecoveryTest {
    @ClassRule
    public static final MiniClusterWithClientResource FLINK =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @Rule
    public final TemporaryFolder savepoints = new TemporaryFolder();

    static final Queue<RecordStateEvent> OUTPUT = new ConcurrentLinkedQueue<>();
    private static final String SENTINEL_ID = "did:plc:sentinel/3k";

    @Test
    public void keyedStateSurvivesStopWithSavepoint() throws Exception {
        OUTPUT.clear();
        JobClient first = start(new Configuration(), "first-source",
                index -> index == 0 ? Records.post(Records.NEWER, "delete") : null);
        awaitOutput(1);
        String savepoint = first
                .stopWithSavepoint(false, savepoints.getRoot().toURI().toString(),
                        SavepointFormatType.CANONICAL)
                .get();

        Configuration restore = new Configuration();
        restore.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
        // The replaced source leaves unclaimed state behind; the keyed state must not be.
        restore.set(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, true);
        JobClient second = start(restore, "second-source", index -> switch (index.intValue()) {
            case 0 -> Records.post(Records.OLDER, "create");
            case 1 -> Records.record(Records.POST, SENTINEL_ID, Records.OLDER, "create");
            default -> null;
        });
        awaitOutput(2);
        second.cancel().get();

        List<RecordStateEvent> output = List.copyOf(OUTPUT);
        assertEquals(2, output.size());
        assertEquals("deleted", output.get(0).state);
        assertEquals(SENTINEL_ID, output.get(1).entityId);
    }

    /**
     * An unbounded generator keeps the job running so it can be stopped with a
     * savepoint; indexes the scenario leaves unset become a filler record.
     */
    private static JobClient start(
            Configuration configuration,
            String sourceUid,
            GeneratorFunction<Long, RecordStateEvent> scenario) throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        environment.setParallelism(1);
        GeneratorFunction<Long, RecordStateEvent> generator = index -> {
            RecordStateEvent event = scenario.map(index);
            return event != null ? event
                    : Records.record("filler", "filler", Records.OLDER, "create");
        };
        DataGeneratorSource<RecordStateEvent> source = new DataGeneratorSource<>(
                generator, Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(100),
                TypeInformation.of(RecordStateEvent.class));
        CurrentStateJob
                .currentStateUpdates(environment
                        .fromSource(source, WatermarkStrategy.noWatermarks(), sourceUid)
                        .uid(sourceUid))
                .filter(event -> !"filler".equals(event.entityType))
                .sinkTo(new CollectingSink());
        return environment.executeAsync("savepoint-recovery-" + sourceUid);
    }

    private static void awaitOutput(int size) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        while (OUTPUT.size() < size) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("expected " + size + " records, got " + OUTPUT);
            }
            Thread.sleep(50);
        }
    }

    private static final class CollectingSink implements Sink<RecordStateEvent> {
        @Override
        public SinkWriter<RecordStateEvent> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                @Override
                public void write(RecordStateEvent event, Context ignored) {
                    OUTPUT.add(event);
                }

                @Override
                public void flush(boolean endOfInput) {}

                @Override
                public void close() {}
            };
        }
    }
}
