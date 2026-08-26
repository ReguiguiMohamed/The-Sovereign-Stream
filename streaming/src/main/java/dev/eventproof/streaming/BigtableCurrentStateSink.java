package dev.eventproof.streaming;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import java.io.IOException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

/**
 * Writes accepted current-state updates to Bigtable, one row per entity.
 *
 * <p>Only serializable configuration is held here; the client is built in the
 * writer because Flink ships this object to the task managers.
 */
public final class BigtableCurrentStateSink implements Sink<OperationalStateEvent> {
    private static final long serialVersionUID = 1L;

    private final String projectId;
    private final String instanceId;
    private final String tableId;
    // 0 means "no emulator": build settings with the normal credential and TLS
    // defaults. Never derive that from a null host, which a misconfiguration
    // could produce silently.
    private final int emulatorPort;

    public BigtableCurrentStateSink(String projectId, String instanceId, String tableId) {
        this(projectId, instanceId, tableId, 0);
    }

    private BigtableCurrentStateSink(
            String projectId, String instanceId, String tableId, int emulatorPort) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.tableId = tableId;
        this.emulatorPort = emulatorPort;
    }

    /** Connects to a local emulator on {@code port} without credentials. */
    public static BigtableCurrentStateSink forEmulator(
            String projectId, String instanceId, String tableId, int port) {
        if (port <= 0) {
            throw new IllegalArgumentException("emulator port must be positive");
        }
        return new BigtableCurrentStateSink(projectId, instanceId, tableId, port);
    }

    @Override
    public SinkWriter<OperationalStateEvent> createWriter(WriterInitContext context)
            throws IOException {
        BigtableDataSettings.Builder settings = emulatorPort > 0
                ? BigtableDataSettings.newBuilderForEmulator(emulatorPort)
                : BigtableDataSettings.newBuilder();
        return new Writer(
                BigtableDataClient.create(
                        settings.setProjectId(projectId).setInstanceId(instanceId).build()),
                tableId);
    }

    /** One Bigtable client per writer, closed when Flink closes the writer. */
    private static final class Writer implements SinkWriter<OperationalStateEvent> {
        private final BigtableDataClient client;
        private final String tableId;

        Writer(BigtableDataClient client, String tableId) {
            this.client = client;
            this.tableId = tableId;
        }

        @Override
        public void write(OperationalStateEvent event, Context context) throws IOException {
            // ponytail: one synchronous mutation per record. Batched or async writes
            // are worth adding only once EP-017 measures throughput; the mutation is
            // idempotent, so a retry of the same event rewrites the same cell.
            client.mutateRow(CurrentStateRow.mutation(tableId, event));
        }

        @Override
        public void flush(boolean endOfInput) {
            // write() is synchronous, so no mutation is ever left buffered here.
        }

        @Override
        public void close() {
            client.close();
        }
    }
}
