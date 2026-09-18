package dev.eventproof.streaming;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

/**
 * Writes accepted updates to Bigtable with the conditional row contract.
 *
 * <p>Only serializable configuration is held here; the client is built in the
 * writer because Flink ships this object to the task managers.
 */
public final class BigtableCurrentStateSink implements Sink<RecordStateEvent> {
    private static final long serialVersionUID = 1L;

    private final String projectId;
    private final String instanceId;
    private final String tableId;
    private final String emulatorHost;

    /** {@code emulatorHost} is {@code host:port}, or null for the real service. */
    public BigtableCurrentStateSink(
            String projectId, String instanceId, String tableId, String emulatorHost) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.tableId = tableId;
        this.emulatorHost = emulatorHost;
    }

    @Override
    public SinkWriter<RecordStateEvent> createWriter(WriterInitContext context)
            throws IOException {
        return new Writer(
                BigtableDataClient.create(
                        CurrentStateRow.settings(projectId, instanceId, emulatorHost)),
                tableId);
    }

    /**
     * Conditional writes run concurrently, which is safe because each one compares
     * revisions on the server. Flink flushes at every checkpoint, and a flush waits
     * for every write, so a checkpoint never covers an unwritten update.
     */
    static final class Writer implements SinkWriter<RecordStateEvent> {
        private static final int MAX_IN_FLIGHT = 256;

        private final BigtableDataClient client;
        private final String tableId;
        private final Semaphore inFlight = new Semaphore(MAX_IN_FLIGHT);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        Writer(BigtableDataClient client, String tableId) {
            this.client = client;
            this.tableId = tableId;
        }

        @Override
        public void write(RecordStateEvent event, Context context)
                throws IOException, InterruptedException {
            throwIfFailed();
            if (CurrentStateRow.purges(event)) {
                // Earlier writes land first, so the purge sees them.
                flush(false);
                client.checkAndMutateRow(CurrentStateRow.purgeMarker(tableId, event));
                CurrentStateRow.purgeRecords(client, tableId, event);
            }
            if (!JetstreamMapping.SYNC.equals(event.entityType)) {
                submit(client.checkAndMutateRowAsync(CurrentStateRow.write(tableId, event)));
                // The index only names the row; what the API serves is still read
                // from the row itself, so a purge hides the record here too.
                submit(client.mutateRowAsync(CurrentStateRow.activity(tableId, event)));
            }
        }

        private void submit(ApiFuture<?> write) throws InterruptedException {
            inFlight.acquire();
            ApiFutures.addCallback(
                    write,
                    new ApiFutureCallback<Object>() {
                        @Override
                        public void onFailure(Throwable error) {
                            failure.compareAndSet(null, error);
                            inFlight.release();
                        }

                        @Override
                        public void onSuccess(Object ignored) {
                            inFlight.release();
                        }
                    },
                    MoreExecutors.directExecutor());
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            inFlight.acquire(MAX_IN_FLIGHT);
            inFlight.release(MAX_IN_FLIGHT);
            throwIfFailed();
        }

        private void throwIfFailed() throws IOException {
            Throwable error = failure.get();
            if (error != null) {
                throw new IOException("Bigtable mutation failed", error);
            }
        }

        @Override
        public void close() {
            client.close();
        }
    }
}
