package dev.eventproof.streaming;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * The single JSON mapping for operational-state.v1.
 *
 * <p>Parsing and Bigtable storage share it so a stored event reads back with the
 * same contract field names it arrived with. Two mappers would let the wire form
 * and the stored form drift apart.
 */
final class EventJson {
    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            // One line carries exactly one contract event: a second value on the
            // line, an unknown field, or a fractional number truncated into an
            // integer field are all silent data loss, so each must fail loudly.
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
            .build();

    private EventJson() {}

    static OperationalStateEvent read(String json) throws JsonProcessingException {
        return MAPPER.readValue(json, OperationalStateEvent.class);
    }

    static String write(OperationalStateEvent event) throws JsonProcessingException {
        return MAPPER.writeValueAsString(event);
    }
}
