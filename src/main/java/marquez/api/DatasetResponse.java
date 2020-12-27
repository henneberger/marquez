package marquez.api;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import marquez.common.models.DatasetId;
import marquez.common.models.DatasetName;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.service.models.Dataset;
import marquez.service.models.Run;
import marquez.service.models.Tag;

public class DatasetResponse {
  private Dataset dataset;

  public DatasetResponse(Dataset dataset) {
    this.dataset = dataset;
  }

  public String getSourceName() {
    return dataset.getSource().getName();
  }

  public String getType() {
    return dataset.getType();
  }

  public String getName() {
    return dataset.getName();
  }

  public String getPhysicalName() {
    return dataset.getPhysicalName();
  }

  public Instant getCreatedAt() {
    return dataset.getCreatedAt();
  }

  public Instant getUpdatedAt() {
    return dataset.getUpdatedAt();
  }

  @NotNull
  public List<FieldResponse> getFields() {
    if (dataset.getFields() == null) {
      return null;
    }
    return dataset.getFields().stream()
        .map(FieldResponse::new).collect(
        Collectors.toList());
  }

  public String getNamespace() {
    return dataset.getNamespace().getName();
  }

  public DatasetId getId() {
    return new DatasetId(NamespaceName.of(dataset.getNamespace().getName()), DatasetName.of(
        dataset.getName()));
  }

  public Set<String> getTags() {
    if (dataset.getTags() == null) {
      return null;
    }
    return dataset.getTags().stream().map(Tag::getName).collect(Collectors.toSet());
  }

  public Optional<Instant> getLastModifiedAt() {
    return dataset.getLastModifiedAt();
  }

  @Nullable
  public Optional<String> getDescription() {
    return dataset.getDescription();
  }

  public Optional<RunId> getRunId() {
    if (dataset.getCurrentVersion() == null) {
      return Optional.empty();
    }
    Optional<Run> run = dataset.getCurrentVersion().getRun();
    if (run == null) {
      return null;
    }
    return run.map(r->new RunId(r.getUuid()));
  }
}
