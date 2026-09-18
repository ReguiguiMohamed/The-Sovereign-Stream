package dev.eventproof.streaming;

import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.threeten.bp.Duration;

/**
 * Query API contract tests against the bundled emulator and a loopback server, both
 * on dynamic ports.
 */
public class CurrentStateApiTest {
    private static final String PROJECT = "eventproof-local";
    private static final String INSTANCE = "eventproof-local";
    private static final String TABLE = "current-state";
    private static final String ENTITY_TYPE = Records.POST;
    private static final String ENTITY_ID = Records.POST_ID;

    private static final InetSocketAddress LOOPBACK =
            new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);

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
                    .addFamily(CurrentStateRow.FAMILY, GCRULES.maxVersions(1))
                    .addFamily(CurrentStateRow.ACTIVITY, GCRULES.maxAge(Duration.ofHours(1))));
        }
        data = BigtableDataClient.create(CurrentStateRow.settings(PROJECT, INSTANCE, host()));
        api = CurrentStateApi.start(PROJECT, INSTANCE, TABLE, LOOPBACK, host());
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
        RecordStateEvent stored = Records.post(Records.OLDER, "create");
        write(stored);

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
        HttpResponse<String> response = get(ENTITY_TYPE, "did:plc:absent/3k");

        assertEquals(404, response.statusCode());
        assertEquals("{\"error\":\"not_found\"}", response.body());
    }

    @Test
    public void missingBlankOrRepeatedParametersAreRejected() throws Exception {
        for (String query : new String[] {
            "",
            "?entity_type=a",
            "?entity_id=c",
            "?entity_type=&entity_id=c",
            "?entity_type=a&entity_id=",
            "?entity_type=a&entity_type=b&entity_id=c",
        }) {
            HttpResponse<String> response = send(HttpRequest.newBuilder(uri(query)).GET().build());
            assertEquals("query: " + query, 400, response.statusCode());
            assertEquals("{\"error\":\"invalid_request\"}", response.body());
        }
    }

    @Test
    public void aNonGetRequestIsRejectedWithAllowGet() throws Exception {
        HttpResponse<String> response = send(HttpRequest
                .newBuilder(uri("?entity_type=a&entity_id=c"))
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
        RecordStateEvent stored =
                Records.record(ENTITY_TYPE, awkwardId, Records.OLDER, "create");
        write(stored);

        HttpResponse<String> response = get(ENTITY_TYPE, awkwardId);

        assertEquals(200, response.statusCode());
        assertEquals(EventJson.write(stored), response.body());
        assertTrue(response.body().contains(awkwardId));
    }

    @Test
    public void theResponseIsTheNewestEventAfterALateWrite() throws Exception {
        RecordStateEvent newer = Records.post(Records.NEWER, "delete");
        RecordStateEvent late = Records.post(Records.OLDER, "create");
        write(newer);
        write(late);

        HttpResponse<String> response = get(ENTITY_TYPE, ENTITY_ID);

        assertEquals(200, response.statusCode());
        assertEquals(EventJson.write(newer), response.body());
    }

    @Test
    public void anInactiveAccountHidesItsRecordsUntilReactivated() throws Exception {
        write(Records.post(Records.OLDER, "create"));
        write(Records.account(Records.DID, 40, "deactivated"));
        assertEquals(404, get(ENTITY_TYPE, ENTITY_ID).statusCode());

        write(Records.account(Records.DID, 41, "active"));
        assertEquals(200, get(ENTITY_TYPE, ENTITY_ID).statusCode());
        assertEquals(200, get("account", Records.DID).statusCode());
    }

    /**
     * The feed is ordered by receive time, holds only entries inside the retention
     * window whatever asynchronous collection has done with the rows, and reports each
     * entity as its row now reads, so a record the account hides leaves the feed.
     */
    @Test
    public void activityIsNewestFirstWindowedAndDropsWhatTheRowHides() throws Exception {
        Instant now = Instant.now();
        RecordStateEvent expired = received(Records.DID + "/3mvq3hy4fsl2n",
                now.minus(CurrentStateRow.ACTIVITY_WINDOW).minusSeconds(60));
        RecordStateEvent older = received(Records.POST_ID, now.minusSeconds(120));
        RecordStateEvent newer = received(Records.DID + "/3mvq3hy4fsl2m", now.minusSeconds(60));
        try (BigtableCurrentStateSink.Writer writer = new BigtableCurrentStateSink.Writer(
                BigtableDataClient.create(CurrentStateRow.settings(PROJECT, INSTANCE, host())),
                TABLE)) {
            for (RecordStateEvent event : List.of(expired, older, newer)) {
                writer.write(event, null);
            }
            writer.flush(false);
        }

        String body = send(HttpRequest.newBuilder(activity(10)).GET().build()).body();
        assertTrue(body, body.contains("\"latest_event_at\":\"" + newer.receivedAt + "\""));
        assertTrue(body, body.indexOf(newer.entityId) < body.indexOf(older.entityId));
        // Its row is still there; the read window is what leaves it out.
        assertFalse(body, body.contains(expired.entityId));

        write(Records.account(Records.DID, 40, "deactivated"));

        body = send(HttpRequest.newBuilder(activity(10)).GET().build()).body();
        assertTrue(body, body.contains("\"records\":[]"));
    }

    /** Milliseconds, because that is the precision a cell timestamp keeps. */
    private static RecordStateEvent received(String entityId, Instant at) {
        RecordStateEvent event =
                Records.record(ENTITY_TYPE, entityId, Records.OLDER, "create");
        event.receivedAt = at.truncatedTo(ChronoUnit.MILLIS).toString();
        return event;
    }

    @Test
    public void theDashboardIsServedAtTheRoot() throws Exception {
        HttpResponse<String> response = send(HttpRequest
                .newBuilder(URI.create("http://127.0.0.1:" + api.port() + "/"))
                .GET()
                .build());

        assertEquals(200, response.statusCode());
        assertEquals(
                "text/html; charset=utf-8",
                response.headers().firstValue("Content-Type").orElseThrow());
        assertTrue(response.body().contains("/v1/activity"));
    }

    @Test
    public void closingTheApiStopsTheListener() throws Exception {
        CurrentStateApi extra =
                CurrentStateApi.start(PROJECT, INSTANCE, TABLE, LOOPBACK, host());
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

    private URI activity(int limit) {
        return URI.create("http://127.0.0.1:" + api.port() + "/v1/activity?limit=" + limit);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }


    private void write(RecordStateEvent event) throws Exception {
        data.checkAndMutateRow(CurrentStateRow.write(TABLE, event));
    }

    private String host() {
        return "localhost:" + emulator.getPort();
    }
}
