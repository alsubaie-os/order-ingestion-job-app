package com.example.ingest.service;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Settings under ingestion.* in application.yml. */
@ConfigurationProperties(prefix = "ingestion")
public record IngestionProperties(
        String sparkHome,
        String jobJar,
        String mainClass,
        String master,
        String driverMemory,
        String bootstrapServers,
        String topic,
        String warehouse,
        String namespace,
        String table,
        Duration jobTimeout,
        String logDir) {
}
