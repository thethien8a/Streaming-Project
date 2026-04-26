FROM flink:1.20

RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar && \
    wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.20.1/flink-sql-avro-confluent-registry-1.20.1.jar

# Pre-create checkpoint/savepoint dirs with correct ownership so the named
# Docker volumes inherit flink:flink permissions on first init.
RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints \
    && chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints

