package com.example.ingest.service;

import java.net.URI;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IngestionController {

    private final JobLauncherService launcher;
    private final IcebergTableService iceberg;

    public IngestionController(JobLauncherService launcher, IcebergTableService iceberg) {
        this.launcher = launcher;
        this.iceberg = iceberg;
    }

    @PostMapping("/jobs")
    public ResponseEntity<JobRun> startJob() {
        JobRun run = launcher.start();
        return ResponseEntity.accepted().location(URI.create("/jobs/" + run.runTs())).body(run);
    }

    // Job status is kept only for this service instance
    @GetMapping("/jobs/{runTs}")
    public JobRun job(@PathVariable String runTs) {
        return launcher.get(runTs);
    }

    @GetMapping("/runs")
    public List<IcebergTableService.RunInfo> runs() {
        return iceberg.listRuns();
    }

    @PostMapping("/runs/rollback/{snapshotId}")
    public IcebergTableService.RollbackResult rollback(@PathVariable long snapshotId) {
        return launcher.whileNoJobRunning(() -> iceberg.rollback(snapshotId));
    }
}
