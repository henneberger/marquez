package marquez.api;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import marquez.common.models.DatasetId;
import marquez.common.models.DatasetName;
import marquez.common.models.Field;
import marquez.common.models.NamespaceName;
import marquez.service.models.Dataset;
import marquez.service.models.Tag;

public class DatasetContract {
  private Dataset dataset;

  public DatasetContract(Dataset dataset) {
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

  public List<Field> getFields() {
    if (dataset.getFields() == null) {
      return null;
    }
    return dataset.getFields().stream()
        .map(f->new Field(f.getName(), f.getType(), f.getTags(), f.getDescription())).collect(
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
    return dataset.getTags().stream().map(Tag::getName).collect(Collectors.toSet());
  }
}
