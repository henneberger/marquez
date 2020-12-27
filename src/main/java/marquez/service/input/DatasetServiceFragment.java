package marquez.service.input;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import marquez.service.input.DatasetInputFragment.FieldFragment;
import marquez.service.models.DatasetVersion;
import marquez.service.models.JobVersion;

@AllArgsConstructor
@Builder
@Getter
public class DatasetServiceFragment {
  private String type;
  private String name;
  private String physicalName;
  private Optional<String> description;
  private SourceFragment source;
  private List<FieldFragment> fields;
  private List<JobVersion> jobVersionAsInput;
  private List<JobVersion> jobVersionAsOutput;
  private NamespaceFragment namespace;
  private DatasetVersion currentVersion;
  private UUID version;
  private Optional<RunFragment> runFragment;
  private List<TagFragment> tagFragments;

  @AllArgsConstructor
  @Builder
  @Getter
  public static class RunFragment {
    private UUID uuid;
  }

  @AllArgsConstructor
  @Builder
  @Getter
  public static class SourceFragment {
    private UUID uuid;
    private String name;
  }

  @AllArgsConstructor
  @Builder
  @Getter
  public static class NamespaceFragment {
    private UUID uuid;
    private String name;
  }
  @AllArgsConstructor
  @Builder
  @Getter
  public static class TagFragment {
    private String name;
  }
}
