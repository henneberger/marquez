package marquez.api;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import marquez.service.models.DatasetField;
import marquez.service.models.Tag;

public class FieldResponse {
  private final DatasetField field;

  public FieldResponse(DatasetField field) {
    this.field = field;
  }

  public String getName() {
    return field.getName();
  }

  public String getType() {
    return field.getType();
  }

  public Set<String> getTags() {
    if (field.getTags() == null) {
      return null;
    }
    return field.getTags().stream().map(Tag::getName).collect(Collectors.toSet());
  }

  public Optional<String> getDescription() {
    return field.getDescription();
  }
}
