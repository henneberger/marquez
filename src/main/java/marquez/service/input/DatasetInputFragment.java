package marquez.service.input;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Delegate;
import marquez.service.input.DatasetServiceFragment.RunFragment;

@Getter
@Builder
@AllArgsConstructor
public class DatasetInputFragment {
  private List<TagFragment> tags;
  private Instant now;

  @Builder.Default
  private Optional<DatasetVersionIdFragment> datasetVersionIdFragment = Optional.empty();
  @Delegate
  private DatasetServiceFragment fragment;

  @Getter
  @Builder
  @AllArgsConstructor
  public static class DatasetVersionIdFragment {
    private UUID uuid;
  }
  @Getter
  @Builder
  @AllArgsConstructor
  public static class DatasetVersionFragment {
    private List<FieldFragment> fieldFragments;
    private RunFragment runFragment;
  }
  @Getter
  @Builder
  @AllArgsConstructor
  public static class FieldFragment {
    private String type;
    private String name;
    private Optional<String> description;
    private List<DatasetServiceFragment.TagFragment> tagFragments;
  }

  @Getter
  @Builder
  @AllArgsConstructor
  public static class TagFragment {
    public UUID uuid;
    public String name;
  }
}
