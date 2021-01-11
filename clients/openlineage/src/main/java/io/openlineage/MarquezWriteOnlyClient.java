package io.openlineage;

import io.openlineage.models.DatasetMeta;
import io.openlineage.models.LineageEvent;
import io.openlineage.models.NamespaceMeta;
import io.openlineage.models.RunState;
import io.openlineage.models.SourceMeta;
import java.io.Closeable;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import io.openlineage.models.JobMeta;
import io.openlineage.models.RunMeta;

/**
 * The contract of a write only client to instrument jobs actions. Can be taken synchronously or
 * asynchronously
 */
public interface MarquezWriteOnlyClient extends Closeable {

  public void emit(LineageEvent event);
  public CompletableFuture emitAsync(LineageEvent event);

  public void createNamespace(String namespaceName, NamespaceMeta namespaceMeta);

  public void createSource(String sourceName, SourceMeta sourceMeta);

  public void createDataset(String namespaceName, String datasetName, DatasetMeta datasetMeta);

  public void createJob(String namespaceName, String jobName, JobMeta jobMeta);

  public void createRun(String namespaceName, String jobName, RunMeta runMeta);

  public void markRunAs(String runId, RunState runState, Instant at);

  public default void markRunAsRunning(String runId) {
    markRunAsRunning(runId, null);
  }

  public default void markRunAsRunning(String runId, Instant at) {
    markRunAs(runId, RunState.RUNNING, at);
  }

  public default void markRunAsCompleted(String runId) {
    markRunAsCompleted(runId, null);
  }

  public default void markRunAsCompleted(String runId, Instant at) {
    markRunAs(runId, RunState.COMPLETED, at);
  }

  public default void markRunAsAborted(String runId) {
    markRunAsAborted(runId, null);
  }

  public default void markRunAsAborted(String runId, Instant at) {
    markRunAs(runId, RunState.ABORTED, at);
  }

  public default void markRunAsFailed(String runId) {
    markRunAsFailed(runId, null);
  }

  public default void markRunAsFailed(String runId, Instant at) {
    markRunAs(runId, RunState.FAILED, at);
  }
}
