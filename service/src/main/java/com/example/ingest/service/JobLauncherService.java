package com.example.ingest.service;

import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * Starts the Spark job in its own process and tracks its status.
 * Only one job runs at a time, so Kafka messages are never read twice,
 * and rollback is refused (409) while a job is running.
 */
@Service
public class JobLauncherService {

    private static final Logger log = LoggerFactory.getLogger(JobLauncherService.class);

    private final IngestionProperties properties;
    private final IcebergTableService iceberg;

    private final ExecutorService waiters = Executors.newCachedThreadPool();
    private final Map<String, JobRun> runs = new ConcurrentHashMap<>();

    private final Object lock = new Object();
    // The run_ts of the job running now, or null when no job is running.
    // Read or change it only inside synchronized (lock).
    private String activeRunTs;

    public JobLauncherService(IngestionProperties properties, IcebergTableService iceberg) {
        this.properties = properties;
        this.iceberg = iceberg;
    }

    /** Creates a new run_ts (this is also the job ID) and starts the job in the background. */
    public JobRun start() {
        synchronized (lock) {
            if (activeRunTs != null) {
                throw alreadyRunning();
            }
            String runTs = Instant.now().truncatedTo(ChronoUnit.MILLIS).toString();
            Path logFile = Path.of(properties.logDir(), "run-" + runTs.replace(':', '-') + ".log");

            Process process;
            try {
                process = launch(runTs, logFile);
            } catch (IOException e) {
                throw new UncheckedIOException("Could not start spark-submit", e);
            }

            JobRun run = JobRun.started(runTs, logFile.toString());
            try {
                waiters.execute(() -> awaitCompletion(run, process));
            } catch (RuntimeException e) {
                stopProcess(process);
                throw e;
            }
            runs.put(runTs, run);
            activeRunTs = runTs;
            log.info("Started run {} (log: {})", runTs, logFile);
            return run;
        }
    }

    public JobRun get(String runTs) {
        JobRun run = runs.get(runTs);
        if (run == null) {
            throw notFound(runTs);
        }
        return run;
    }

    /** Runs the action (for example a rollback) only when no job is running. */
    public <T> T whileNoJobRunning(Supplier<T> action) {
        synchronized (lock) {
            if (activeRunTs != null) {
                throw alreadyRunning();
            }
            return action.get();
        }
    }

    private Process launch(String runTs, Path logFile) throws IOException {
        Files.createDirectories(logFile.getParent());
        return new SparkLauncher()
                .setSparkHome(properties.sparkHome())
                .setAppResource(properties.jobJar())
                .setMainClass(properties.mainClass())
                .setMaster(properties.master())
                .setAppName("order-ingestion " + runTs)
                .setConf(SparkLauncher.DRIVER_MEMORY, properties.driverMemory())
                .setConf("spark.ui.enabled", "false")
                .addAppArgs(
                        "--run-ts", runTs,
                        "--bootstrap-servers", properties.bootstrapServers(),
                        "--topic", properties.topic(),
                        "--warehouse", properties.warehouse(),
                        "--table", properties.namespace() + "." + properties.table())
                .redirectError()
                .redirectOutput(logFile.toFile())
                .launch();
    }

    /** Runs in the background: waits for Spark to finish, then saves the final status. */
    private JobRun awaitCompletion(JobRun run, Process process) {
        JobRun result = run.failed(null, "Unexpected error while waiting for the job");
        try {
            Integer exitCode = null;
            String problem = null;
            if (!process.waitFor(properties.jobTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                stopProcess(process);
                problem = "Timed out after " + properties.jobTimeout() + "; process killed";
            } else {
                exitCode = process.exitValue();
                if (exitCode != 0) {
                    problem = "spark-submit exited with code " + exitCode + "; see " + run.logFile();
                }
            }

            Optional<IcebergTableService.RunInfo> committed = iceberg.findRun(run.runTs());
            if (committed.isPresent()) {
                String note = problem == null ? null
                        : "Rows and offsets were committed, but the job then failed: " + problem;
                result = run.succeeded(committed.get(), exitCode, note);
            } else {
                result = run.failed(exitCode, problem != null ? problem
                        : "Job exited with code 0 but no snapshot has run_ts " + run.runTs());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stopProcess(process);
            result = run.failed(null, "Service shut down while the job was running");
        } catch (RuntimeException e) {
            result = run.failed(null, "Job finished but reading its snapshot failed: " + e.getMessage());
        } finally {
            synchronized (lock) {
                runs.put(run.runTs(), result);
                activeRunTs = null;
            }
        }
        log.info("Run {} finished: {}", run.runTs(), result.status());
        return result;
    }

    // Stops Spark and waits until it stopped.
    private static void stopProcess(Process process) {
        process.destroyForcibly();
        boolean interrupted = false;
        while (process.isAlive()) {
            try {
                process.waitFor();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private ResponseStatusException alreadyRunning() {
        return new ResponseStatusException(HttpStatus.CONFLICT,
                "Run " + activeRunTs + " is still in progress. Wait for it to finish (GET /jobs/" + activeRunTs + ").");
    }

    private static ResponseStatusException notFound(String runTs) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND,
                "No job launched by this service instance has run_ts " + runTs);
    }

    // When the service shuts down, stop all running Spark jobs.
    @PreDestroy
    void shutdown() {
        waiters.shutdownNow();
    }
}
