package marquez.service.input;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import marquez.common.models.JobType;
import org.postgresql.util.PGobject;

@Builder
@Getter
@AllArgsConstructor
public class JobServiceFragment {
  private final String namespace;
  private final String jobName;
  private final JobType type;
  private final Set<DatasetFragment> inputs;
  private final Set<DatasetFragment> outputs;
  private final String location;
  private final Map<String, String> context;
  private final Optional<String> description;
  private final Optional<UUID> runId;
  @Builder
  @Getter
  @AllArgsConstructor
  public static class DatasetFragment extends PGobject {
    private final String namespace;
    private final String datasetName;
    public String getValue() {
      return "(" + datasetName + "," + datasetName + ")"; //todo escape
    }
  }
}