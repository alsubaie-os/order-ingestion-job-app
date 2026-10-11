# syntax=docker/dockerfile:1

# Build the Spark job JAR.
FROM maven:3.9-eclipse-temurin-17 AS job-build
WORKDIR /build
COPY job/pom.xml .
COPY job/src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp package

# Build the Spring Boot service JAR.
FROM maven:3.9-eclipse-temurin-17 AS service-build
WORKDIR /build
COPY service/pom.xml .
COPY service/src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp package

# Run the API and separate Spark jobs as user 185, with writable storage in /data.
FROM apache/spark:3.5.9-scala2.12-java17-ubuntu
USER root
COPY --from=job-build /build/target/order-ingestion-job.jar /app/order-ingestion-job.jar
COPY --from=service-build /build/target/order-ingestion-service.jar /app/order-ingestion-service.jar
RUN mkdir -p /data && chown -R 185:185 /app /data
USER 185
WORKDIR /app
ENV SPARK_HOME=/opt/spark
EXPOSE 8080
ENTRYPOINT ["java", "-Xmx384m", "-jar", "/app/order-ingestion-service.jar"]
