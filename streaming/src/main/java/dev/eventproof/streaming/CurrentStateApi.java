package dev.eventproof.streaming;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-only current-state lookup for one entity: {@code GET
 * /v1/current-state?entity_type=&entity_id=}.
 *
 * <p>Every request is one Bigtable point read of one row. There is deliberately no
 * scan, listing, pagination or mutation, so the query cost cannot grow with the
 * size of the table.
 *
 * <p>Binds to loopback. It carries no authentication because it is a local proof;
 * exposing it beyond loopback would need one first.
 */
public final class CurrentStateApi implements AutoCloseable {
    private static final String PATH = "/v1/current-state";
    private static final String JSON = "application/json; charset=utf-8";

    private final HttpServer server;
    private final BigtableDataClient client;
    private final String tableId;

    private CurrentStateApi(HttpServer server, BigtableDataClient client, String tableId) {
        this.server = server;
        this.client = client;
        this.tableId = tableId;
    }

    /** {@code emulatorPort} of 0 means a real instance with the usual credentials. */
    public static CurrentStateApi start(
            String projectId, String instanceId, String tableId, int httpPort, int emulatorPort)
            throws IOException {
        BigtableDataSettings.Builder settings = emulatorPort > 0
                ? BigtableDataSettings.newBuilderForEmulator(emulatorPort)
                : BigtableDataSettings.newBuilder();
        BigtableDataClient client = BigtableDataClient.create(
                settings.setProjectId(projectId).setInstanceId(instanceId).build());

        HttpServer server = HttpServer.create(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), httpPort), 0);
        CurrentStateApi api = new CurrentStateApi(server, client, tableId);
        server.createContext(PATH, api::handle);
        server.start();
        return api;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
        client.close();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET");
                respond(exchange, 405, "{\"error\":\"method_not_allowed\"}");
                return;
            }
            // createContext matches by prefix, so anything deeper is not this endpoint.
            if (!PATH.equals(exchange.getRequestURI().getPath())) {
                respond(exchange, 404, "{\"error\":\"not_found\"}");
                return;
            }
            Map<String, List<String>> query = parseQuery(exchange.getRequestURI().getRawQuery());
            String entityType = single(query, "entity_type");
            String entityId = single(query, "entity_id");
            if (entityType == null || entityId == null) {
                respond(exchange, 400, "{\"error\":\"invalid_request\"}");
                return;
            }
            OperationalStateEvent current =
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

    private static void respond(HttpExchange exchange, int status, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", JSON);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream response = exchange.getResponseBody()) {
            response.write(bytes);
        }
    }

    public static void main(String[] args) throws IOException {
        String emulatorHost = System.getenv("BIGTABLE_EMULATOR_HOST");
        try (CurrentStateApi api = start(
                require("BIGTABLE_PROJECT_ID"),
                require("BIGTABLE_INSTANCE_ID"),
                require("BIGTABLE_TABLE_ID"),
                Integer.parseInt(System.getenv().getOrDefault("PORT", "8080")),
                emulatorHost == null
                        ? 0
                        : Integer.parseInt(emulatorHost.substring(emulatorHost.indexOf(':') + 1)))) {
            System.out.println("current-state API on http://127.0.0.1:" + api.port() + PATH);
            Thread.currentThread().join();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static String require(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " must be set");
        }
        return value;
    }
}
