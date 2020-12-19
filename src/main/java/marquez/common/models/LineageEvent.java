package marquez.common.models;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;
import lombok.ToString;

@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class LineageEvent {
  //todo: add a generated id component
  public String transition;
  public ZonedDateTime transitionTime;

  public LineageRun run;
  public LineageJob job;

  public OriginFacet origin;

  public List<LineageDataset> inputs;
  public List<LineageDataset> outputs;

  public EventFacet facets;

  @ToString
  @NotNull
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class LineageRun {
    @NotNull
    public String runId;
    public RunFacet facets;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RunFacet {
    public NominalTimeFacet nominalTime;
    public LineageRunParent parent;
    public Map<String, Object> additional = new HashMap<>();
    @JsonAnySetter
    void setFacet(String key, Object value) {
      additional.put(key, value);
    }
    //todo any getter to flatten facets
  }

  public static class LineageRunParent {
//    "runId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
//    "jobId": {
//      "namespace": "my-scheduler-namespace",
//          "name": "myjob.mytask"
//    }
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class NominalTimeFacet {
    public ZonedDateTime nominalStartTime;
    public ZonedDateTime nominalEndTime;
  }

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
    @JsonAnySetter
    void setFacet(String key, Object value) {
      additional.put(key, value);
    }
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

  @ToString
  @NotNull
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class LineageDataset {
    public String namespace; @NotNull
    public String name; @NotNull
    public DatasetFacet facets;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DatasetFacet {
    public SchemaFacet schema;
    public DataSourceFacet dataSource;
    public String description;
    public Map<String, Object> additional = new HashMap<>();
    @JsonAnySetter
    void setFacet(String key, Object value) {
      additional.put(key, value);
    }
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
    @JsonAnySetter
    void setFacet(String key, Object value) {
      additional.put(key, value);
    }
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class OriginFacet {
    public String name;
    public String version;
  }
}
