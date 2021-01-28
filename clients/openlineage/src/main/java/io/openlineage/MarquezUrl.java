package io.openlineage;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static io.openlineage.MarquezPathV1.createRunPath;
import static io.openlineage.MarquezPathV1.datasetPath;
import static io.openlineage.MarquezPathV1.datasetTagPath;
import static io.openlineage.MarquezPathV1.fieldTagPath;
import static io.openlineage.MarquezPathV1.jobPath;
import static io.openlineage.MarquezPathV1.lineagePath;
import static io.openlineage.MarquezPathV1.listDatasetsPath;
import static io.openlineage.MarquezPathV1.listJobsPath;
import static io.openlineage.MarquezPathV1.listNamespacesPath;
import static io.openlineage.MarquezPathV1.listRunsPath;
import static io.openlineage.MarquezPathV1.listSourcesPath;
import static io.openlineage.MarquezPathV1.listTagsPath;
import static io.openlineage.MarquezPathV1.namespacePath;
import static io.openlineage.MarquezPathV1.runPath;
import static io.openlineage.MarquezPathV1.runTransitionPath;
import static io.openlineage.MarquezPathV1.sourcePath;

import com.google.inject.internal.util.ImmutableMap;
import java.net.URISyntaxException;
import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;
import io.openlineage.models.RunState;
import org.apache.hc.core5.net.URIBuilder;

class MarquezUrl {

  static MarquezUrl create(URL url) {
    return new MarquezUrl(url);
  }

  public final URI baseUri;

  MarquezUrl(final URL baseUrl) {
    this.baseUri = Utils.toUri(baseUrl);
  }


  URI from(String path) {
    return from(path, new HashMap<>());
  }


  URI from(String path, Map<String, Object> queryParams) {
    try {
      final URIBuilder builder = new URIBuilder(baseUri).setPath(baseUri.getPath() + path);
      if (queryParams != null) {
        queryParams.forEach((name, value) -> builder.addParameter(name, String.valueOf(value)));
      }
      return builder.build();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "can not build url from parameters: " + path + " " + queryParams, e);
    }
  }

  private Map<String, Object> newQueryParamsWith(int limit, int offset) {
    if (limit >= 0) {
      throw new RuntimeException("limit must be >= 0");
    }
    if (offset >= 0) {
      throw new RuntimeException("offset must be >= 0");
    }
    return ImmutableMap.of("limit", limit, "offset", offset);
  }

  URI toListNamespacesUri(int limit, int offset) {
    return from(listNamespacesPath(), newQueryParamsWith(limit, offset));
  }

  URI toNamespaceUri(String namespaceName) {
    return from(namespacePath(namespaceName));
  }

  URI toSourceUri(String sourceName) {
    return from(sourcePath(sourceName));
  }

  URI toDatasetUri(String namespaceName, String datasetName) {
    return from(datasetPath(namespaceName, datasetName));
  }

  URI toListJobsUri(@NonNull String namespaceName, int limit, int offset) {
    return from(listJobsPath(namespaceName), newQueryParamsWith(limit, offset));
  }

  URI toJobUri(String namespaceName, String jobName) {
    return from(jobPath(namespaceName, jobName));
  }

  URI toCreateRunUri(String namespaceName, String jobName) {
    return from(createRunPath(namespaceName, jobName));
  }

  URI toRunUri(@NonNull String runId) {
    return from(runPath(runId));
  }

  URI toListRunsUri(@NonNull String namespaceName, @NonNull String jobName, int limit, int offset) {
    return from(listRunsPath(namespaceName, jobName), newQueryParamsWith(limit, offset));
  }

  URI toRunTransitionUri(String runId, RunState runState, Instant at) {
    return from(
        runTransitionPath(runId, runState),
        at == null ? ImmutableMap.of() : ImmutableMap.of("at", ISO_INSTANT.format(at)));
  }

  URI toListSourcesUri(int limit, int offset) {
    return from(listSourcesPath(), newQueryParamsWith(limit, offset));
  }

  URI toListDatasetsUri(@NonNull String namespaceName, int limit, int offset) {
    return from(listDatasetsPath(namespaceName), newQueryParamsWith(limit, offset));
  }

  URI toDatasetTagUri(
      @NonNull String namespaceName, @NonNull String datasetName, @NonNull String tagName) {
    return from(datasetTagPath(namespaceName, datasetName, tagName));
  }

  URI toFieldTagUri(String namespaceName, String datasetName, String fieldName, String tagName) {
    return from(fieldTagPath(namespaceName, datasetName, fieldName, tagName));
  }

  URI toListTagsUri(int limit, int offset) {
    return from(listTagsPath(), newQueryParamsWith(limit, offset));
  }

  URI toLineageUri() {
    return from(lineagePath());
  }
}
