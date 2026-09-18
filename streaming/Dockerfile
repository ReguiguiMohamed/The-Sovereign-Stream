# Built by cloudbuild.yaml after `mvn verify` has produced target/eventproof-streaming.jar.

# Flink job image for the Flink Kubernetes Operator; GCS plugin for checkpoints.
FROM flink:2.2.1-java17@sha256:13b1328bbb86263b41e740f06790f49c8512a26a5c0eba36a8f91b1983d33a6c AS flink-job
RUN mkdir -p /opt/flink/plugins/gs-fs-hadoop \
    && cp /opt/flink/opt/flink-gs-fs-hadoop-2.2.1.jar /opt/flink/plugins/gs-fs-hadoop/
COPY target/eventproof-streaming.jar /opt/flink/usrlib/eventproof-streaming.jar

# Producer and query API: the main class is the container argument.
FROM eclipse-temurin:17-jre-noble@sha256:4e04629bf268320c6b33d178ebebe0d830bf7ddd141b12e6100006f979621b9a AS service
COPY target/eventproof-streaming.jar /app/eventproof-streaming.jar
USER 65532
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-cp", "/app/eventproof-streaming.jar"]
