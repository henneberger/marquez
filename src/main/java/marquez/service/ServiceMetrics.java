package marquez.service;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import lombok.NonNull;
import marquez.common.models.RunState;

public interface ServiceMetrics {
  static final Counter sources =
      Counter.build()
          .namespace("marquez")
          .name("source_total")
          .help("Total number of sources.")
          .register();
  static final Counter datasets =
      Counter.build()
          .namespace("marquez")
          .name("dataset_total")
          .labelNames("namespace_name", "dataset_type")
          .help("Total number of datasets.")
          .register();
  static final Counter versions =
      Counter.build()
          .namespace("marquez")
          .name("dataset_versions_total")
          .labelNames("namespace_name", "dataset_type", "dataset_name")
          .help("Total number of dataset versions.")
          .register();
  public static final Counter jobs =
      Counter.build()
          .namespace("marquez")
          .name("job_total")
          .labelNames("namespace_name", "job_type")
          .help("Total number of jobs.")
          .register();
  public static final Counter job_versions =
      Counter.build()
          .namespace("marquez")
          .name("job_versions_total")
          .labelNames("namespace_name", "job_type", "job_name")
          .help("Total number of job versions.")
          .register();
  public static final Gauge runsActive =
      Gauge.build()
          .namespace("marquez")
          .name("job_runs_active")
          .help("Total number of active job runs.")
          .register();
  public static final Gauge runsCompleted =
      Gauge.build()
          .namespace("marquez")
          .name("job_runs_completed")
          .help("Total number of completed job runs.")
          .register();
  static final Counter namespaces =
      Counter.build()
          .namespace("marquez")
          .name("namespace_total")
          .help("Total number of namespaces.")
          .register();

  public static void emitVersionMetric(String namespaceName, String jobMetaType, String jobName) {
    job_versions.labels(namespaceName, jobMetaType, jobName).inc();
  }

  public static void emitJobCreationMetric(String namespaceName, String jobMetaType) {
    jobs.labels(namespaceName, jobMetaType).inc();
  }

  /** Determines whether to increment or decrement run counters given {@link RunState}. */
  public static void emitRunStateCounterMetric(@NonNull RunState runState) {
    switch (runState) {
      case NEW:
        break;
      case RUNNING:
        runsActive.inc();
        break;
      case COMPLETED:
        runsActive.dec();
        runsCompleted.inc();
        break;
      case ABORTED:
      case FAILED:
        runsActive.dec();
        break;
    }
  }
}
