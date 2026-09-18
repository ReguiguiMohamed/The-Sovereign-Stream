package dev.eventproof.streaming;

import static dev.eventproof.streaming.KafkaSettings.require;

import dev.eventproof.streaming.TransactionalPublisher.Received;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.net.http.WebSocketHandshakeException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Reads Bluesky Jetstream v2 and publishes it with {@link TransactionalPublisher}.
 *
 * <p>The cursor starts from the last committed record in the topic and advances
 * only on commit. Any Kafka failure ends the process; the restart resumes from the
 * committed cursor, and the transactional id fences the previous instance. The
 * Jetstream cursor is inclusive, so the boundary event is redelivered and dropped
 * downstream by its revision.
 */
public final class JetstreamProducer {
    /** The public Jetstream instances; one is tried per connection attempt. */
    static final List<String> ENDPOINTS = List.of(
            "wss://jetstream.us-east.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents",
            "wss://jetstream.us-west.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents");
    private static final String USER_AGENT =
            "eventproof (+https://github.com/ReguiguiMohamed/The-Sovereign-Stream)";
    private static final int MAX_CONSECUTIVE_FAILURES = 10;

    private static final AtomicLong received = new AtomicLong();
    private static final AtomicLong receivedBytes = new AtomicLong();

    private JetstreamProducer() {}

    static String url(long cursor, int attempt) {
        String collections = JetstreamMapping.COLLECTIONS.stream()
                .map(collection -> "collections=" + collection)
                .collect(Collectors.joining("&"));
        return ENDPOINTS.get(Math.floorMod(attempt, ENDPOINTS.size()))
                + "?" + collections + (cursor < 0 ? "" : "&cursor=" + cursor);
    }

    public static void main(String[] args) throws Exception {
        String topic = require("KAFKA_TOPIC");
        String quarantineTopic = require("KAFKA_QUARANTINE_TOPIC");
        Properties kafka = KafkaSettings.fromEnvironment();

        Properties consumerProperties = new Properties();
        consumerProperties.putAll(kafka);
        consumerProperties.setProperty("isolation.level", "read_committed");
        AtomicLong cursor = new AtomicLong();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                consumerProperties, new StringDeserializer(), new StringDeserializer())) {
            cursor.set(TransactionalPublisher.lastCommittedSeq(consumer, topic));
        }
        System.out.println("{\"resume_cursor\":" + cursor.get() + "}");

        Properties producerProperties = new Properties();
        producerProperties.putAll(kafka);
        producerProperties.setProperty("transactional.id", "jetstream-producer");
        producerProperties.setProperty("compression.type", "zstd");
        producerProperties.setProperty("linger.ms", "20");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                producerProperties, new StringSerializer(), new StringSerializer())) {
            producer.initTransactions();
            TransactionalPublisher publisher =
                    new TransactionalPublisher(producer, topic, quarantineTopic);
            // The counters at the moment the container is stopped, so a run can
            // be compared with what the topic actually holds.
            Runtime.getRuntime().addShutdownHook(
                    new Thread(() -> logStats(publisher, cursor.get())));
            ScheduledExecutorService stats = Executors.newSingleThreadScheduledExecutor(
                    runnable -> {
                        Thread thread = new Thread(runnable, "stats");
                        thread.setDaemon(true);
                        return thread;
                    });
            stats.scheduleAtFixedRate(
                    () -> logStats(publisher, cursor.get()), 1, 1, TimeUnit.MINUTES);
            run(publisher, cursor);
        }
    }

    private static void run(TransactionalPublisher publisher, AtomicLong cursor)
            throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        int failures = 0;
        while (true) {
            // A fresh queue per connection: nothing from an earlier session is published.
            Session session = new Session(
                    new ArrayBlockingQueue<>(4 * TransactionalPublisher.MAX_BATCH));
            WebSocket socket = null;
            try {
                socket = http.newWebSocketBuilder()
                        .header("User-Agent", USER_AGENT)
                        .buildAsync(URI.create(url(cursor.get(), failures)), session)
                        .join();
                List<Received> batch = new ArrayList<>();
                while (!session.closed.isDone() || !session.queue.isEmpty()) {
                    Received first = session.queue.poll(1, TimeUnit.SECONDS);
                    if (first == null) {
                        continue;
                    }
                    batch.add(first);
                    session.queue.drainTo(batch, TransactionalPublisher.MAX_BATCH - 1);
                    cursor.set(publisher.publish(batch, cursor.get()));
                    batch.clear();
                    failures = 0;
                }
                System.err.println("jetstream closed: "
                        + session.closed.handle((ignored, error) -> error).join());
            } catch (CompletionException connectFailure) {
                // A refused handshake carries its status only in the response.
                Throwable cause = connectFailure.getCause();
                System.err.println("jetstream connection failed: "
                        + (cause instanceof WebSocketHandshakeException refused
                                ? "HTTP " + refused.getResponse().statusCode() : cause));
            } finally {
                if (socket != null) {
                    socket.abort();
                }
                // Unblocks a listener waiting on a full queue of the dead session.
                session.queue.clear();
            }
            failures++;
            if (failures > MAX_CONSECUTIVE_FAILURES) {
                throw new IllegalStateException(
                        "jetstream unavailable after " + MAX_CONSECUTIVE_FAILURES + " attempts");
            }
            System.err.println("reconnecting from cursor " + cursor.get() + " to "
                    + ENDPOINTS.get(Math.floorMod(failures, ENDPOINTS.size())));
            Thread.sleep(Math.min(60_000L, 1_000L << failures));
        }
    }

    private static void logStats(TransactionalPublisher publisher, long cursor) {
        System.out.printf(
                "{\"received\":%d,\"received_bytes\":%d,\"published\":%d,"
                        + "\"identity_skipped\":%d,\"quarantined\":%d,\"cursor\":%d}%n",
                received.get(), receivedBytes.get(), publisher.published.get(),
                publisher.skipped.get(), publisher.quarantined.get(), cursor);
    }

    private static final class Session implements WebSocket.Listener {
        final CompletableFuture<Void> closed = new CompletableFuture<>();
        final BlockingQueue<Received> queue;
        private final StringBuilder frame = new StringBuilder();

        Session(BlockingQueue<Received> queue) {
            this.queue = queue;
        }

        @Override
        public void onOpen(WebSocket socket) {
            socket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket socket, CharSequence data, boolean last) {
            frame.append(data);
            if (last) {
                String message = frame.toString();
                frame.setLength(0);
                received.incrementAndGet();
                receivedBytes.addAndGet(message.getBytes(StandardCharsets.UTF_8).length);
                try {
                    // Blocks while the queue is full, which pauses reading.
                    queue.put(new Received(message, Instant.now()));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    socket.abort();
                    return null;
                }
            }
            socket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket socket, int statusCode, String reason) {
            closed.complete(null);
            return null;
        }

        @Override
        public void onError(WebSocket socket, Throwable error) {
            closed.completeExceptionally(error);
        }
    }
}
