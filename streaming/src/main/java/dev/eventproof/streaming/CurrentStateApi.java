package dev.eventproof.streaming;

import static dev.eventproof.streaming.KafkaSettings.require;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Read-only current state: {@code GET /v1/current-state?entity_type=&entity_id=}
 * for one entity, {@code GET /v1/activity?limit=} for the entities changed most
 * recently, and the dashboard that uses both at {@code /}.
 *
 * <p>A lookup is one Bigtable point read of one row. Activity reads at most
 * {@code limit} rows of the recent-activity index, whose keys carry the
 * complement of the event time, and then one point read per distinct entity, so
 * neither query scans the table nor grows with its size.
 *
 * <p>The service has no authentication of its own: on Cloud Run it is deployed
 * without public access, so only IAM-authorized callers reach it.
 */
public final class CurrentStateApi implements AutoCloseable {
    private static final String PATH = "/v1/current-state";
    private static final String ACTIVITY_PATH = "/v1/activity";
    private static final String JSON = "application/json; charset=utf-8";
    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 200;
    /** Matches the Cloud Run concurrency setting, so requests never queue here. */
    static final int WORKERS = 16;
    /** Classpath resource, served at the root. */
    private static final String DASHBOARD = "/dashboard.html";

    private final HttpServer server;
    private final ExecutorService workers;
    private final BigtableDataClient client;
    private final String tableId;

    private CurrentStateApi(
            HttpServer server, ExecutorService workers, BigtableDataClient client, String tableId) {
        this.server = server;
        this.workers = workers;
        this.client = client;
        this.tableId = tableId;
    }

    /** {@code emulatorHost} is {@code host:port}, or null for the real service. */
    public static CurrentStateApi start(
            String projectId,
            String instanceId,
            String tableId,
            InetSocketAddress address,
            String emulatorHost)
            throws IOException {
        BigtableDataClient client = BigtableDataClient.create(
                CurrentStateRow.settings(projectId, instanceId, emulatorHost));

        HttpServer server = HttpServer.create(address, 0);
        ExecutorService workers = Executors.newFixedThreadPool(WORKERS);
        server.setExecutor(workers);
        CurrentStateApi api = new CurrentStateApi(server, workers, client, tableId);
        server.createContext(PATH, api::handle);
        server.createContext(ACTIVITY_PATH, api::handle);
        server.createContext("/", api::handle);
        server.start();
        return api;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    /** Stops accepting, lets in-flight requests finish within the Cloud Run grace period. */
    @Override
    public void close() {
        server.stop(5);
        workers.shutdown();
        client.close();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET");
                respond(exchange, 405, "{\"error\":\"method_not_allowed\"}");
                return;
            }
            String path = exchange.getRequestURI().getPath();
            Map<String, List<String>> query = parseQuery(exchange.getRequestURI().getRawQuery());
            if (ACTIVITY_PATH.equals(path)) {
                respond(exchange, 200, activity(limit(single(query, "limit"))));
                return;
            }
            if ("/".equals(path)) {
                respondHtml(exchange, dashboard());
                return;
            }
            // createContext matches by prefix, so anything deeper is not an endpoint.
            if (!PATH.equals(path)) {
                respond(exchange, 404, "{\"error\":\"not_found\"}");
                return;
            }
            String entityType = single(query, "entity_type");
            String entityId = single(query, "entity_id");
            if (entityType == null || entityId == null) {
                respond(exchange, 400, "{\"error\":\"invalid_request\"}");
                return;
            }
            RecordStateEvent current =
                    CurrentStateRow.read(client, tableId, entityType, entityId);
            if (current == null) {
                respond(exchange, 404, "{\"error\":\"not_found\"}");
                return;
            }
            respond(exchange, 200, EventJson.write(current));
        } catch (RuntimeException | IOException failure) {
            // A caller learns that the request failed, never why.
            respond(exchange, 500, "{\"error\":\"internal\"}");
        } finally {
            exchange.close();
        }
    }

    /**
     * The entities changed most recently, newest first, each as its own row
     * currently reads, so a purged or hidden record is left out rather than listed.
     */
    private String activity(int limit) throws IOException {
        Set<String> keys = new LinkedHashSet<>();
        Instant latest = null;
        for (Row row : client.readRows(CurrentStateRow.recent(tableId, limit))) {
            if (latest == null) {
                latest = CurrentStateRow.activityTime(row.getKey().toStringUtf8());
            }
            keys.add(row.getCells().get(0).getValue().toStringUtf8());
        }
        StringBuilder records = new StringBuilder("[");
        for (String key : keys) {
            String[] entity = CurrentStateRow.parseRowKey(key);
            RecordStateEvent current =
                    CurrentStateRow.read(client, tableId, entity[0], entity[1]);
            if (current == null) {
                continue;
            }
            if (records.length() > 1) {
                records.append(',');
            }
            records.append(EventJson.write(current));
        }
        return "{\"observed_at\":\"" + Instant.now()
                + "\",\"latest_event_at\":"
                + (latest == null ? "null" : "\"" + latest + "\"")
                + ",\"records\":" + records.append(']') + "}";
    }

    /** Bounded, so one request can never read an unbounded number of rows. */
    static int limit(String raw) {
        if (raw == null) {
            return DEFAULT_LIMIT;
        }
        try {
            return Math.max(1, Math.min(MAX_LIMIT, Integer.parseInt(raw)));
        } catch (NumberFormatException notANumber) {
            return DEFAULT_LIMIT;
        }
    }

    private static byte[] dashboard() throws IOException {
        try (InputStream page = CurrentStateApi.class.getResourceAsStream(DASHBOARD)) {
            return page.readAllBytes();
        }
    }

    /** Returns the one non-blank value, or null when absent, blank or repeated. */
    private static String single(Map<String, List<String>> query, String name) {
        List<String> values = query.get(name);
        if (values == null || values.size() != 1 || values.get(0).isBlank()) {
            return null;
        }
        return values.get(0);
    }

    private static Map<String, List<String>> parseQuery(String rawQuery) {
        Map<String, List<String>> query = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return query;
        }
        for (String pair : rawQuery.split("&")) {
            int equals = pair.indexOf('=');
            String name = decode(equals < 0 ? pair : pair.substring(0, equals));
            String value = equals < 0 ? "" : decode(pair.substring(equals + 1));
            query.computeIfAbsent(name, key -> new ArrayList<>()).add(value);
        }
        return query;
    }

    private static String decode(String raw) {
        return URLDecoder.decode(raw, StandardCharsets.UTF_8);
    }

    private static void respondHtml(HttpExchange exchange, byte[] page) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        exchange.sendResponseHeaders(200, page.length);
        try (OutputStream response = exchange.getResponseBody()) {
            response.write(page);
        }
    }

    private static void respond(HttpExchange exchange, int status, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", JSON);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream response = exchange.getResponseBody()) {
            response.write(bytes);
        }
    }

    /** Container entrypoint: listens on every interface at {@code PORT}. */
    public static void main(String[] args) throws IOException {
        CurrentStateApi api = start(
                require("BIGTABLE_PROJECT_ID"),
                require("BIGTABLE_INSTANCE_ID"),
                require("BIGTABLE_TABLE_ID"),
                new InetSocketAddress(Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"))),
                System.getenv("BIGTABLE_EMULATOR_HOST"));
        // Cloud Run sends SIGTERM before stopping the instance.
        Runtime.getRuntime().addShutdownHook(new Thread(api::close));
        System.out.println("current-state API listening on port " + api.port());
    }
}
