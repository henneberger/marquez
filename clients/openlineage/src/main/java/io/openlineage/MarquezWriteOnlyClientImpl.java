package io.openlineage;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static io.openlineage.MarquezPathV1.createRunPath;
import static io.openlineage.MarquezPathV1.datasetPath;
import static io.openlineage.MarquezPathV1.jobPath;
import static io.openlineage.MarquezPathV1.lineagePath;
import static io.openlineage.MarquezPathV1.namespacePath;
import static io.openlineage.MarquezPathV1.runTransitionPath;
import static io.openlineage.MarquezPathV1.sourcePath;

import com.google.inject.internal.util.ImmutableMap;
import io.openlineage.models.DatasetMeta;
import io.openlineage.models.LineageEvent;
import io.openlineage.models.NamespaceMeta;
import io.openlineage.models.RunState;
import io.openlineage.models.SourceMeta;
import java.io.IOException;
import java.net.URLEncoder;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import io.openlineage.models.JobMeta;
import io.openlineage.models.RunMeta;

class MarquezWriteOnlyClientImpl implements MarquezWriteOnlyClient {

  private final Backend backend;

  public MarquezWriteOnlyClientImpl(Backend backend) {
    this.backend = backend;
  }

  private static String path(String path, Map<String, Object> queryParams) {
    StringBuilder pathBuilder = new StringBuilder();
    pathBuilder.append(path);
    if (queryParams != null && !queryParams.isEmpty()) {
      boolean first = true;
      for (Entry<String, Object> entry : queryParams.entrySet()) {
        if (first) {
          pathBuilder.append("?");
          first = false;
        } else {
          pathBuilder.append("&");
        }
        String paramName = URLEncoder.encode(entry.getKey());
        String paramValue = URLEncoder.encode(String.valueOf(entry.getValue()));
        pathBuilder.append(paramName).append("=").append(paramValue);
      }
    }
    return pathBuilder.toString();
  }

  @Override
  public void createNamespace(String namespaceName, NamespaceMeta namespaceMeta) {
    backend.put(namespacePath(namespaceName), namespaceMeta);
  }

  @Override
  public void createSource(String sourceName, SourceMeta sourceMeta) {
    backend.put(sourcePath(sourceName), sourceMeta);
  }

  @Override
  public void createDataset(String namespaceName, String datasetName, DatasetMeta datasetMeta) {
    backend.put(datasetPath(namespaceName, datasetName), datasetMeta);
  }

  @Override
  public void createJob(String namespaceName, String jobName, JobMeta jobMeta) {
    backend.put(jobPath(namespaceName, jobName), jobMeta);
  }

  @Override
  public void createRun(String namespaceName, String jobName, RunMeta runMeta) {
    backend.post(createRunPath(namespaceName, jobName), runMeta);
  }

  @Override
  public void markRunAs(String runId, RunState runState, Instant at) {
    Map<String, Object> queryParams =
        at == null ? null : ImmutableMap.of("at", ISO_INSTANT.format(at));
    backend.post(path(runTransitionPath(runId, runState), queryParams));
  }

  @Override
  public void emit(LineageEvent event) {
    backend.post(lineagePath(), event);
  }

  @Override
  public CompletableFuture emitAsync(LineageEvent event) {
    return backend.postAsync(lineagePath(), event);
  }

  @Override
  public void close() throws IOException {
    backend.close();
  }
}
