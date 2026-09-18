package dev.eventproof.streaming;

import static dev.eventproof.streaming.Records.NEWER;
import static dev.eventproof.streaming.Records.OLDER;
import static dev.eventproof.streaming.Records.POST;
import static dev.eventproof.streaming.Records.post;
import static dev.eventproof.streaming.Records.record;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;
import org.junit.ClassRule;
import org.junit.Test;

public class CurrentStateJobTest {
    @ClassRule
    public static final MiniClusterWithClientResource FLINK =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Instant RECEIVED = Instant.parse("2026-09-17T16:34:45Z");
    private static final String LIKE = """
            {"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit",\
            "cid":"bafyreidxlr7k4vsuak5c6ddbh3z4eyr3qtnc23r5ylvkldeutr7gfbfbda",\
            "collection":"app.bsky.feed.like","did":"did:plc:v2rolw535iyn5dgqidht3r3v",\
            "operation":"create","record":{"$type":"app.bsky.feed.like","createdAt":"2026-09-17T16:34:44.100Z",\
            "subject":{"cid":"bafyreicq7qfejzbet5xgwwecjej3ccqrmnoqsfjlh56xwpprglkjohnnrq",\
            "uri":"at://did:plc:az4ipcibnh2kdwmy4i7jcf63/app.bsky.feed.post/3mvq3ibdde22u"}},\
            "rev":"3mvq3ibybj72k","rkey":"3mvq3iby6lh2k","seq":25984172978,\
            "time":"2026-09-17T16:34:44.687783Z"}}""";

    @Test
    public void redeliveredRevisionProducesOneUpdate() throws Exception {
        List<RecordStateEvent> output = run(post(OLDER, "create"), post(OLDER, "create"));

        assertEquals(1, output.size());
    }

    @Test
    public void olderRevisionArrivingLastCannotReplaceNewer() throws Exception {
        List<RecordStateEvent> output = run(post(NEWER, "delete"), post(OLDER, "create"));

        assertEquals(1, output.size());
        assertEquals("deleted", output.get(0).state);
    }

    @Test
    public void deleteAfterCreateBecomesCurrent() throws Exception {
        List<RecordStateEvent> output = run(post(OLDER, "create"), post(NEWER, "delete"));

        assertEquals(2, output.size());
        assertEquals("deleted", output.get(1).state);
    }

    /** Keyed by entity_id alone, the second record would be suppressed as older. */
    @Test
    public void sameIdUnderAnotherTypeIsAnotherRecord() throws Exception {
        String id = Records.DID + "/shared";
        List<RecordStateEvent> output = run(
                record(POST, id, NEWER, "create"),
                record("app.bsky.feed.like", id, OLDER, "create"));

        assertEquals(2, output.size());
    }

    @Test
    public void jetstreamCommitMapsToTheContract() throws Exception {
        RecordStateEvent event = map(LIKE);

        assertEquals("app.bsky.feed.like", event.entityType);
        assertEquals("did:plc:v2rolw535iyn5dgqidht3r3v/3mvq3iby6lh2k", event.entityId);
        assertEquals("3mvq3ibybj72k", event.revision);
        assertEquals(Long.valueOf(25984172978L), event.sourceSeq);
        assertEquals("active", event.state);
        assertEquals(
                "at://did:plc:az4ipcibnh2kdwmy4i7jcf63/app.bsky.feed.post/3mvq3ibdde22u",
                event.subject);
        assertEquals(event.eventId,
                new ParseRecordState().map(EventJson.write(event)).eventId);
        // The stored form carries no record body.
        assertEquals(-1, EventJson.write(event).indexOf("createdAt"));
    }

    @Test
    public void redeliveryKeepsTheEventIdAndDeleteNeedsNoRecord() throws Exception {
        RecordStateEvent first = map(LIKE);
        RecordStateEvent again = JetstreamMapping.toRecordState(
                JSON.readTree(LIKE), Instant.parse("2026-09-17T16:40:00Z"));
        RecordStateEvent delete = map(LIKE
                .replace("\"create\"", "\"delete\"")
                .replaceAll("\"cid\":\"[a-z0-9]+\",", "")
                .replaceAll("\"record\":\\{.*?\\}\\},", ""));

        assertEquals(first.eventId, again.eventId);
        assertEquals("deleted", delete.state);
        assertNull(delete.subject);
        assertNotEquals(first.eventId, delete.eventId);
    }

    @Test
    public void accountAndSyncEventsBecomeAccountMarkers() throws Exception {
        RecordStateEvent deleted = map(account("\"active\":false,\"status\":\"deleted\""));
        RecordStateEvent deactivated =
                map(account("\"active\":false,\"status\":\"deactivated\""));
        RecordStateEvent active = map(account("\"active\":true"));
        RecordStateEvent sync = map(marker("sync", "\"sync\":{\"rev\":\"3mvq3ibybj72k\"}"));

        assertEquals("account", deleted.entityType);
        assertEquals(Records.DID, deleted.entityId);
        assertEquals("00000000025984172978", deleted.revision);
        assertEquals("deleted", deleted.state);
        assertEquals("deactivated", deactivated.state);
        assertEquals("active", active.state);
        assertEquals("sync", sync.entityType);
        assertEquals("resynced", sync.state);
        assertEquals(true, CurrentStateRow.purges(deleted));
        assertEquals(false, CurrentStateRow.purges(deactivated));
        assertEquals(true, CurrentStateRow.purges(sync));
        assertNull(map(marker("identity", "\"identity\":{\"handle\":\"a.bsky.social\"}")));
    }

    @Test
    public void malformedSourceMessagesAreRejected() {
        for (String message : List.of(
                LIKE.replace("\"did\":\"did:plc:v2rolw535iyn5dgqidht3r3v\",", ""),
                LIKE.replace("\"rkey\":\"3mvq3iby6lh2k\"", "\"rkey\":\"a/b\""),
                LIKE.replace("25984172978", "25984172978.5"),
                LIKE.replace("25984172978", "\"25984172978\""),
                LIKE.replace("25984172978", "-1"),
                LIKE.replace("app.bsky.feed.like\",\"did\"", "app.bsky.graph.follow\",\"did\""),
                LIKE.replace("\"create\"", "\"upsert\""),
                LIKE.replace("\"3mvq3ibybj72k\"", "\"not-a-tid\""),
                LIKE.replace("#commit", "#unknown"),
                account("\"status\":\"deleted\""),
                account("\"active\":false"),
                marker("account", "\"active\":true"))) {
            assertThrows(message, IllegalArgumentException.class, () -> map(message));
        }
    }

    @Test
    public void parserRejectsContractViolations() throws Exception {
        ParseRecordState parser = new ParseRecordState();
        String line = EventJson.write(post(OLDER, "create"));
        assertEquals(OLDER, parser.map(line).revision);

        assertThrows(JsonProcessingException.class, () -> parser.map(line + line));
        assertThrows(JsonProcessingException.class,
                () -> parser.map(line.replace("\"state\":", "\"unexpected\":1,\"state\":")));
        assertThrows(JsonProcessingException.class,
                () -> parser.map(line.replace("25984168574", "1.5")));
        for (String broken : List.of(
                line.replace("record-state.v1", "record-state.v0"),
                line.replace("25984168574", "-1"),
                line.replace("25984168574", "null"),
                line.replace(OLDER, "3MVQ3HY4JPL2L"),
                line.replace("\"active\"", "\"deleted\""),
                line.replace("\"create\"", "\"upsert\""),
                line.replace(Records.POST_ID, "/3mvq3hy4fsl2l"),
                line.replace(POST, "app.bsky.graph.follow"))) {
            assertThrows(broken, IllegalArgumentException.class, () -> parser.map(broken));
        }
        String account = EventJson.write(Records.account(Records.DID, 7, "deleted"));
        assertEquals("deleted", parser.map(account).state);
        assertThrows(IllegalArgumentException.class,
                () -> parser.map(account.replace("00000000000000000007", "7")));
    }

    @Test
    public void rowKeySeparatorCannotCollideAcrossEntityTypeAndId() {
        assertEquals("app.bsky.feed.post#did:plc:x/3k", CurrentStateRow.rowKey(POST, "did:plc:x/3k"));
        assertNotEquals(CurrentStateRow.rowKey("a#b", "c"), CurrentStateRow.rowKey("a", "b#c"));
    }

    private static RecordStateEvent map(String message) throws Exception {
        return JetstreamMapping.toRecordState(JSON.readTree(message), RECEIVED);
    }

    /** The account status is nested under "account", as the source sends it. */
    private static String account(String status) {
        return marker("account", "\"account\":{\"did\":\"" + Records.DID + "\","
                + "\"seq\":33706760865,\"time\":\"2026-09-17T16:34:44.100Z\"," + status + "}");
    }

    private static String marker(String kind, String fields) {
        return "{\"$type\":\"message\",\"payload\":{\"$type\":"
                + "\"network.bsky.jetstream.subscribeEvents#" + kind + "\","
                + "\"did\":\"" + Records.DID + "\",\"seq\":25984172978,"
                + "\"time\":\"2026-09-17T16:34:44.687783Z\"," + fields + "}}";
    }

    private static List<RecordStateEvent> run(RecordStateEvent... events) throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        List<RecordStateEvent> output = new ArrayList<>();
        try (CloseableIterator<RecordStateEvent> results = CurrentStateJob
                .currentStateUpdates(environment.fromData(events))
                .executeAndCollect()) {
            results.forEachRemaining(output::add);
        }
        return output;
    }
}
