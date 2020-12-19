package marquez.spark.agent;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@Builder(builderClassName = "LineageEventBuilder", toBuilder = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class LineageEvent {
  //todo: add a generated id component
  public String transition;
  public long transitionTime;

  public LineageRun run;
  public LineageJob job;

  public OriginFacet origin;

  public List<LineageDataset> inputs;
  public List<LineageDataset> outputs;

  public EventFacet facets;

  @Data
  @Builder
  @ToString
  @AllArgsConstructor
  @NoArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class LineageRun {
    public String runId;
    public RunFacet facets;
  }

  @Data
  @Builder
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RunFacet {
    public NominalTimeFacet nominalTime;
    public LineageRunParent parent;
    public Map<String, Object> additional = new HashMap<>();
    //todo getter to flatten facets
  }

  public static class LineageRunParent {
    public String runId;
    public RunParentJobId job;
  }

  public static class RunParentJobId {
    public String namespace;
    public String name;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class NominalTimeFacet {
    public ZonedDateTime nominalStartTime;
    public ZonedDateTime nominalEndTime;
  }

  @Data
  @Builder
  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class LineageJob {
    public String namespace;
    public String name;
    public JobFacet facets;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class JobFacet {
    public SourceCodeLocationFacet sourceCodeLocation;
    public SqlFacet sql;
    public String description;
    public Map<String, Object> additional = new HashMap<>();
    //todo flatten assets
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SourceCodeLocationFacet {
    public String type;
    public String url;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SqlFacet {
    public String query;
  }

  @Data
  @Builder
  @ToString
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class LineageDataset {
    public String namespace;
    public String name;
    public DatasetFacet facets;
  }

  @Data
  @ToString
  @Builder
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DatasetFacet {
    public SchemaFacet schema;
    public DataSourceFacet dataSource;
    public String description;
    public Map<String, Object> additional = new HashMap<>();
    //todo getter
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SchemaFacet {
    public List<SchemaField> fields;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SchemaField {
    public String name;
    public String type;
    public String description;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DataSourceFacet {
    public String name;
    public String url;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class EventFacet {
    public Map<String, Object> additional = new HashMap<>();
    //todo getter
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class OriginFacet {
    public String name;
    public String version;
  }
}

