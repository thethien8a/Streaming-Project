FROM maven:3.9-eclipse-temurin-11 AS builder

WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src/ src/

RUN mvn package -DskipTests -B


FROM curlimages/curl:latest

COPY --from=builder /app/target/flink-job-submitter-1.0.0.jar /opt/job/flink-job-submitter.jar
COPY submit.sh /opt/job/submit.sh

USER root
RUN sed -i 's/\r$//' /opt/job/submit.sh && chmod +x /opt/job/submit.sh

ENTRYPOINT ["/opt/job/submit.sh"]
