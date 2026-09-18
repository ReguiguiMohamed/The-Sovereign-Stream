package dev.eventproof.streaming;

import java.util.Properties;

/** Client settings shared by the producer and the Flink source. */
final class KafkaSettings {
    private KafkaSettings() {}

    /**
     * Managed Kafka SASL with Google's official login handler and ambient
     * credentials. {@code KAFKA_AUTH=none} selects plaintext, used only by the
     * packaged-image smoke test against its in-build broker.
     */
    static Properties fromEnvironment() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", require("KAFKA_BOOTSTRAP"));
        if ("none".equals(System.getenv("KAFKA_AUTH"))) {
            return properties;
        }
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.mechanism", "OAUTHBEARER");
        properties.setProperty("sasl.login.callback.handler.class",
                "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler");
        properties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        return properties;
    }

    static String require(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " must be set");
        }
        return value;
    }
}
