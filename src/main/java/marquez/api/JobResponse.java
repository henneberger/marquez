package marquez.api;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Type;
import java.net.URL;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import jdk.jfr.Description;
import lombok.Getter;
import marquez.common.Utils;
import marquez.common.models.DatasetId;
import marquez.common.models.DatasetName;
import marquez.common.models.JobId;
import marquez.common.models.JobName;
import marquez.common.models.NamespaceName;
import marquez.service.models.Job;

public class JobResponse {
  private final Job job;
  public JobResponse(Job job) {
    this.job = job;
  }

  public Set<DatasetId> getInputs() {
    if (job.getCurrentVersion() != null && job.getCurrentVersion().getInputs() != null) {
      return job.getCurrentVersion().getInputs()
          .stream().map(d -> new DatasetId(NamespaceName.of(d.getNamespace().getName()),
              DatasetName.of(d.getName())))
          .collect(Collectors.toSet());
    }
    return ImmutableSet.of();
  }
  public Set<DatasetId> getOutputs() {
    if (job.getCurrentVersion() != null && job.getCurrentVersion().getOutputs() != null) {
      return job.getCurrentVersion().getOutputs()
          .stream().map(d -> new DatasetId(NamespaceName.of(d.getNamespace().getName()),
              DatasetName.of(d.getName())))
          .collect(Collectors.toSet());
    }
    return ImmutableSet.of();
  }
  public Optional<String> getLocation() {
    return job.getCurrentVersion().getLocation();
  }
  public Map<String, String> getContext() {
    if (job.getCurrentVersion() != null &&
        job.getCurrentVersion().getJobContext() != null &&
        job.getCurrentVersion().getJobContext().getContext() != null) {
      return Utils.fromJson(job.getCurrentVersion().getJobContext().getContext(),
          new TypeReference<Map<String, String>>() {
          });
    }
    return null;
  }
  public Optional<String> getDescription() {
    return job.getDescription();
  }
  public Optional<UUID> getRunId() {
    if (job.getCurrentVersion().getLatestRun() != null && job.getCurrentVersion().getLatestRun().isPresent()) {
      return Optional.of(job.getCurrentVersion().getLatestRun().get().getUuid());
    }
    return Optional.empty();
  }

  public JobId getId() {
    return new JobId(NamespaceName.of(job.getNamespace().getName()), JobName.of(job.getName()));
  }

  public String getName() {
    return job.getName();
  }

  public Instant getCreatedAt() {
    return job.getCreatedAt();
  }

  public Instant getUpdatedAt() {
    return job.getUpdatedAt();
  }

  public String getType() {
    return job.getType();
  }

  public String getNamespace() {
    return job.getNamespace().getName();
  }

  public RunResponse getLatestRun() {
    if (job.getCurrentVersion() == null || job.getCurrentVersion().getLatestRun() == null) {
      return null;
    }
    return job.getCurrentVersion().getLatestRun().map(RunResponse::new).orElse(null);
  }
}
