package dev.eventproof.streaming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
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

    @Test
    public void exactDuplicateProducesOneCurrentStateUpdate() throws Exception {
        OperationalStateEvent event = event(
                "a".repeat(64), 0, "2026-08-25T00:00:00Z", "processing");

        List<OperationalStateEvent> output = run(event, event);

        assertEquals(1, output.size());
        assertEquals(event.getEventId(), output.get(0).getEventId());
    }

    @Test
    public void parserMapsTheVersionedContractFieldNames() throws Exception {
        String line = """
                {"schema_version":"operational-state.v1","event_id":"%s",\
                "run_id":"parser-test","sequence":7,"source":"synthetic",\
                "entity_type":"resource","entity_id":"resource-0007",\
                "event_type":"state.updated","event_time":"2026-08-25T00:00:00Z",\
                "received_at":"2026-08-25T00:00:01Z","state":"processing"}
                """.formatted("c".repeat(64));

        OperationalStateEvent event = new ParseOperationalState().map(line);

        assertEquals("operational-state.v1", event.schemaVersion);
        assertEquals("resource-0007", event.getEntityId());
        assertEquals("processing", event.getState());
    }

    @Test
    public void parserRejectsAnUnknownSchemaVersion() {
        String line = "{\"schema_version\":\"unknown\"}";

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> new ParseOperationalState().map(line));

        assertEquals("schema_version must be operational-state.v1", error.getMessage());
    }

    @Test
    public void eventThatIs120SecondsLateCannotReplaceNewerState() throws Exception {
        OperationalStateEvent newer = event(
                "a".repeat(64), 0, "2026-08-25T00:00:00Z", "completed");
        OperationalStateEvent late = event(
                "b".repeat(64), 1, "2026-08-24T23:58:00Z", "processing");

        List<OperationalStateEvent> output = run(newer, late);

        assertEquals(1, output.size());
        assertEquals(newer.getEventId(), output.get(0).getEventId());
        assertEquals("completed", output.get(0).getState());
    }

    @Test
    public void parserAcceptsZeroButRejectsEveryOtherSequenceShape() throws Exception {
        ParseOperationalState parser = new ParseOperationalState();

        assertEquals(
                Integer.valueOf(0),
                parser.map(lineWithSequence("\"sequence\":0,")).sequence);

        // A fractional value is a malformed integer, so Jackson rejects it before
        // the contract check runs.
        assertThrows(
                JsonProcessingException.class,
                () -> parser.map(lineWithSequence("\"sequence\":1.5,")));

        // Missing and null both reach the contract check as a null Integer.
        for (String sequence : List.of("", "\"sequence\":null,", "\"sequence\":-1,")) {
            assertThrows(
                    "sequence fragment: " + sequence,
                    IllegalArgumentException.class,
                    () -> parser.map(lineWithSequence(sequence)));
        }
    }

    @Test
    public void parserRejectsASecondObjectOnTheLineAndUnknownFields() {
        ParseOperationalState parser = new ParseOperationalState();
        String line = lineWithSequence("\"sequence\":0,");

        assertThrows(JsonProcessingException.class, () -> parser.map(line + line));
        assertThrows(
                JsonProcessingException.class,
                () -> parser.map(line.replace("\"state\":", "\"unexpected\":1,\"state\":")));
    }

    @Test
    public void rowKeySeparatorCannotCollideAcrossEntityTypeAndId() {
        assertEquals(
                "resource#resource-0000",
                CurrentStateRow.rowKey("resource", "resource-0000"));
        // Without escaping both of these would be "a#b#c".
        assertNotEquals(
                CurrentStateRow.rowKey("a#b", "c"), CurrentStateRow.rowKey("a", "b#c"));
    }

    private static String lineWithSequence(String sequenceField) {
        return "{\"schema_version\":\"operational-state.v1\","
                + "\"event_id\":\"" + "c".repeat(64) + "\","
                + "\"run_id\":\"parser-test\","
                + sequenceField
                + "\"source\":\"synthetic\",\"entity_type\":\"resource\","
                + "\"entity_id\":\"resource-0007\",\"event_type\":\"state.updated\","
                + "\"event_time\":\"2026-08-25T00:00:00Z\","
                + "\"received_at\":\"2026-08-25T00:00:01Z\","
                + "\"state\":\"processing\"}";
    }

    private static List<OperationalStateEvent> run(OperationalStateEvent... events)
            throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        List<OperationalStateEvent> output = new ArrayList<>();
        try (CloseableIterator<OperationalStateEvent> results = CurrentStateJob
                .currentStateUpdates(environment.fromData(events))
                .executeAndCollect()) {
            results.forEachRemaining(output::add);
        }
        return output;
    }

    private static OperationalStateEvent event(
            String eventId, int sequence, String eventTime, String state) {
        return new OperationalStateEvent(
                "operational-state.v1",
                eventId,
                "mini-cluster-test",
                sequence,
                "synthetic",
                "resource",
                "resource-0000",
                "state.updated",
                eventTime,
                "2026-08-25T00:00:01Z",
                state);
    }

}
