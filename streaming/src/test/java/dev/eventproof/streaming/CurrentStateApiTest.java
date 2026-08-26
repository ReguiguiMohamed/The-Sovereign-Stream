package dev.eventproof.streaming;

import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * EP-015 contract tests against the bundled emulator and a loopback server, both
 * on dynamic ports.
 */
public class CurrentStateApiTest {
    private static final String PROJECT = "eventproof-local";
    private static final String INSTANCE = "eventproof-local";
    private static final String TABLE = "current-state";
    private static final String ENTITY_TYPE = "resource";
    private static final String ENTITY_ID = "resource-0000";

    @Rule
    public final BigtableEmulatorRule emulator = BigtableEmulatorRule.create();

    private final HttpClient http = HttpClient.newHttpClient();
    private BigtableDataClient data;
    private CurrentStateApi api;

    @Before
    public void startApi() throws Exception {
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
        api = CurrentStateApi.start(PROJECT, INSTANCE, TABLE, 0, emulator.getPort());
    }

    @After
    public void closeApi() {
        if (api != null) {
            api.close();
        }
        if (data != null) {
            data.close();
        }
    }

    @Test
    public void aStoredEntityIsReturnedInFull() throws Exception {
        OperationalStateEvent stored =
                event("a".repeat(64), "2026-08-25T00:00:00Z", "completed", ENTITY_ID);
        data.mutateRow(CurrentStateRow.mutation(TABLE, stored));

        HttpResponse<String> response = get(ENTITY_TYPE, ENTITY_ID);

        assertEquals(200, response.statusCode());
        assertEquals(
                "application/json; charset=utf-8",
                response.headers().firstValue("Content-Type").orElseThrow());
        // The complete contract event, not a projection of it.
        assertEquals(EventJson.write(stored), response.body());
    }

    @Test
    public void anAbsentEntityIsNotFound() throws Exception {
        HttpResponse<String> response = get(ENTITY_TYPE, "resource-9999");

        assertEquals(404, response.statusCode());
        assertEquals("{\"error\":\"not_found\"}", response.body());
    }

    @Test
    public void missingBlankOrRepeatedParametersAreRejected() throws Exception {
        for (String query : new String[] {
            "",
            "?entity_type=resource",
            "?entity_id=resource-0000",
            "?entity_type=&entity_id=resource-0000",
            "?entity_type=resource&entity_id=",
            "?entity_type=resource&entity_type=other&entity_id=resource-0000",
        }) {
            HttpResponse<String> response = send(HttpRequest.newBuilder(uri(query)).GET().build());
            assertEquals("query: " + query, 400, response.statusCode());
            assertEquals("{\"error\":\"invalid_request\"}", response.body());
        }
    }

    @Test
    public void aNonGetRequestIsRejectedWithAllowGet() throws Exception {
        HttpResponse<String> response = send(HttpRequest
                .newBuilder(uri("?entity_type=resource&entity_id=resource-0000"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build());

        assertEquals(405, response.statusCode());
        assertEquals("GET", response.headers().firstValue("Allow").orElseThrow());
    }

    /**
     * '#' and '%' are the row-key escape characters, so a URL-encoded identifier
     * containing them must still reach the same row the writer produced.
     */
    @Test
    public void urlEncodedIdentifiersResolveThroughTheRowKeyContract() throws Exception {
        String awkwardId = "res #1 100% a/b";
        OperationalStateEvent stored =
                event("b".repeat(64), "2026-08-25T00:00:00Z", "completed", awkwardId);
        data.mutateRow(CurrentStateRow.mutation(TABLE, stored));

        HttpResponse<String> response = get(ENTITY_TYPE, awkwardId);

        assertEquals(200, response.statusCode());
        assertEquals(EventJson.write(stored), response.body());
        assertTrue(response.body().contains(awkwardId));
    }

    @Test
    public void theResponseIsTheNewestEventAfterALateWrite() throws Exception {
        OperationalStateEvent newer =
                event("a".repeat(64), "2026-08-25T00:00:00Z", "completed", ENTITY_ID);
        OperationalStateEvent late =
                event("b".repeat(64), "2026-08-24T23:58:00Z", "processing", ENTITY_ID);
        data.mutateRow(CurrentStateRow.mutation(TABLE, newer));
        data.mutateRow(CurrentStateRow.mutation(TABLE, late));

        HttpResponse<String> response = get(ENTITY_TYPE, ENTITY_ID);

        assertEquals(200, response.statusCode());
        assertEquals(EventJson.write(newer), response.body());
    }

    @Test
    public void closingTheApiStopsTheListener() throws Exception {
        CurrentStateApi extra =
                CurrentStateApi.start(PROJECT, INSTANCE, TABLE, 0, emulator.getPort());
        URI address = URI.create(
                "http://127.0.0.1:" + extra.port() + "/v1/current-state?entity_type=a&entity_id=b");
        extra.close();

        assertThrows(
                IOException.class,
                () -> http.send(
                        HttpRequest.newBuilder(address).GET().build(),
                        HttpResponse.BodyHandlers.ofString()));
    }

    private HttpResponse<String> get(String entityType, String entityId) throws Exception {
        return send(HttpRequest.newBuilder(uri("?entity_type=" + encode(entityType)
                + "&entity_id=" + encode(entityId))).GET().build());
    }

    private HttpResponse<String> send(HttpRequest request) throws Exception {
        return http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private URI uri(String query) {
        return URI.create("http://127.0.0.1:" + api.port() + "/v1/current-state" + query);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static OperationalStateEvent event(
            String eventId, String eventTime, String state, String entityId) {
        return new OperationalStateEvent(
                "operational-state.v1",
                eventId,
                "api-test",
                0,
                "synthetic",
                ENTITY_TYPE,
                entityId,
                "state.updated",
                eventTime,
                "2026-08-25T00:00:01Z",
                state);
    }
}
