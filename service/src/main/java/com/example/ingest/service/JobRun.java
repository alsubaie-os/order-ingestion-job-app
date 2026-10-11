package com.example.ingest.service;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

/**
 * In-memory job status; successful runs are stored in Iceberg snapshots.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record JobRun(
        String runTs,
        Status status,
        Instant startedAt,
        Instant finishedAt,
        Integer exitCode,
        Long snapshotId,
        Long rowsAdded,
        Long rowsRejected,
        String message,
        String logFile) {

    public enum Status { RUNNING, SUCCEEDED, FAILED }

    static JobRun started(String runTs, String logFile) {
        return new JobRun(runTs, Status.RUNNING, Instant.now(), null, null, null, null, null, null, logFile);
    }

    JobRun succeeded(IcebergTableService.RunInfo run, Integer exitCode, String message) {
        return new JobRun(runTs, Status.SUCCEEDED, startedAt, Instant.now(), exitCode,
                run.snapshotId(), run.rowsAdded(), run.rowsRejected(), message, logFile);
    }

    JobRun failed(Integer exitCode, String message) {
        return new JobRun(runTs, Status.FAILED, startedAt, Instant.now(), exitCode,
                null, null, null, message, logFile);
    }
}
