package com.example.ingest.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * Reads the run history from the Iceberg table and rolls the table back.
 * It reads the same table files that the Spark job writes.
 */
@Service
public class IcebergTableService {

    private static final Logger log = LoggerFactory.getLogger(IcebergTableService.class);

    // Snapshot properties: the first three are written by our Spark job, the last two by Iceberg itself.
    private static final String RUN_TS = "run_ts";
    private static final String KAFKA_OFFSETS = "kafka.offsets";
    private static final String ROWS_REJECTED = "rows_rejected";
    private static final String ADDED_RECORDS = "added-records";
    private static final String TOTAL_RECORDS = "total-records";

    /** One entry in GET /runs. */
    public record RunInfo(
            String runTs,
            long snapshotId,
            Long parentSnapshotId,
            Instant committedAt,
            long rowsAdded,
            long rowsRejected,
            JsonNode kafkaOffsets,
            boolean current,
            boolean rolledBack) {
    }

    /** The response of a rollback: the snapshot that is current now. */
    public record RollbackResult(
            long snapshotId,
            String runTs,
            long totalRows) {
    }

    private final HadoopCatalog catalog;
    private final TableIdentifier tableId;
    private final ObjectMapper objectMapper;

    public IcebergTableService(IngestionProperties properties, ObjectMapper objectMapper) {
        this.catalog = new HadoopCatalog(new Configuration(), properties.warehouse());
        this.tableId = TableIdentifier.of(properties.namespace(), properties.table());
        this.objectMapper = objectMapper;
    }

    /** Empty until the first job run creates the table. */
    public Optional<Table> loadTable() {
        if (!catalog.tableExists(tableId)) {
            return Optional.empty();
        }
        return Optional.of(catalog.loadTable(tableId));
    }

    /** Returns all runs (snapshots that have a run_ts), oldest first. */
    public List<RunInfo> listRuns() {
        Optional<Table> maybeTable = loadTable();
        if (maybeTable.isEmpty()) {
            return List.of();
        }
        Table table = maybeTable.get();

        Snapshot current = table.currentSnapshot();
        // Snapshots in the current history are visible; any other run was removed by a rollback.
        Set<Long> visible = current == null ? Set.of() : new HashSet<>(SnapshotUtil.currentAncestorIds(table));

        List<RunInfo> result = new ArrayList<>();
        for (Snapshot snapshot : table.snapshots()) {
            if (snapshot.summary().containsKey(RUN_TS)) {
                result.add(toRunInfo(snapshot, current, visible));
            }
        }
        result.sort(Comparator.comparing(RunInfo::committedAt));
        return result;
    }

    /** Finds the run with this run_ts, if it was saved to the table. */
    public Optional<RunInfo> findRun(String runTs) {
        return listRuns().stream().filter(run -> run.runTs().equals(runTs)).findFirst();
    }

    /**
     * Moves the table back to an earlier run. The data and the Kafka offsets of that run become
     * current again, so the next job reloads the messages after it. No files are deleted.
     * Only runs in the current history are allowed.
     */
    public RollbackResult rollback(long snapshotId) {
        Table table = loadTable().orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                "Table " + tableId + " does not exist yet; run the job first"));

        Snapshot target = table.snapshot(snapshotId);
        if (target == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,
                    "Snapshot " + snapshotId + " not found in " + tableId);
        }
        if (!SnapshotUtil.currentAncestorIds(table).contains(snapshotId)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Snapshot " + snapshotId
                    + " is not an ancestor of the current snapshot; only a rollback to an earlier state is supported");
        }

        long currentSnapshotId = table.currentSnapshot().snapshotId();
        if (currentSnapshotId != snapshotId) {
            table.manageSnapshots().rollbackTo(snapshotId).commit();
            log.info("Table {} rolled back from snapshot {} to {}", tableId, currentSnapshotId, snapshotId);
        }

        Map<String, String> summary = target.summary();
        return new RollbackResult(snapshotId, summary.get(RUN_TS), parseLong(summary.get(TOTAL_RECORDS)));
    }

    /** Builds one GET /runs entry from a snapshot. */
    private RunInfo toRunInfo(Snapshot snapshot, Snapshot current, Set<Long> visible) {
        Map<String, String> summary = snapshot.summary();
        return new RunInfo(
                summary.get(RUN_TS),
                snapshot.snapshotId(),
                snapshot.parentId(),
                Instant.ofEpochMilli(snapshot.timestampMillis()),
                parseLong(summary.get(ADDED_RECORDS)),
                parseLong(summary.get(ROWS_REJECTED)),
                parseJson(summary.get(KAFKA_OFFSETS)),
                current != null && current.snapshotId() == snapshot.snapshotId(),
                !visible.contains(snapshot.snapshotId()));
    }

    // A missing count means zero (for example, a run that added no rows).
    private static long parseLong(String value) {
        return value == null ? 0L : Long.parseLong(value);
    }

    // Reads the stored offsets as JSON. If they are not valid JSON, returns them as plain text.
    private JsonNode parseJson(String value) {
        if (value == null) {
            return null;
        }
        try {
            return objectMapper.readTree(value);
        } catch (JsonProcessingException e) {
            return objectMapper.getNodeFactory().textNode(value);
        }
    }

    // Closes the catalog when the service stops.
    @PreDestroy
    void close() throws IOException {
        catalog.close();
    }
}
