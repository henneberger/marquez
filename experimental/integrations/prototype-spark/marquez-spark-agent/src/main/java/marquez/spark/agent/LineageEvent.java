package marquez.spark.agent;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.ToString;
import lombok.Builder;

/**
 * Requires jackson serialization features: mapper.registerModule(new JavaTimeModule());
 * mapper.setSerializationInclusion(Include.NON_NULL);
 * mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
 * mapper.disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
 */
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
@Builder
public class LineageEvent {

  public String eventType;
  @NonNull
  public ZonedDateTime eventTime;
  @NonNull
  public LineageRun run;
  @NonNull public LineageJob job;
  public List<LineageDataset> inputs;
  public List<LineageDataset> outputs;
  @NonNull public String producer;

  @ToString
  @NonNull
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Builder
  public static class LineageRun {

    @NonNull public String runId;
    public RunFacet facets;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPropertyOrder({"nominalTime", "parent"})
  @Builder
  public static class RunFacet {

    public NominalTimeFacet nominalTime;
    public LineageRunParent parent;

    @Builder.Default
    private Map<String, Object> additional = new LinkedHashMap<>();

    @JsonAnySetter
    void setFacet(String key, Object value) {
      additional.put(key, value);
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalFacets() {
      return additional;
    }
  }

  public abstract static class BaseFacet {
    @NonNull public URI _producer;
    @NonNull public URI _schemaURL;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Builder
  public static class NominalTimeFacet extends BaseFacet {

    @NonNull public ZonedDateTime nominalStartTime;
    public ZonedDateTime nominalEndTime;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Builder
  public static class LineageRunParent extends BaseFacet {
    @NonNull public RunLink run;
    @NonNull public JobLink job;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Builder
  public static class RunLink {
    @NonNull public String runId;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Builder
  public static class JobLink {
    @NonNull public String namespace;
    @NonNull public String name;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Builder
  public static class LineageJob {

    @NonNull public String namespace;
    @NonNull public String name;
    public JobFacet facets;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPropertyOrder({"documentation", "sourceCodeLocation", "sql", "description"})
  @Builder
  public static class JobFacet extends BaseFacet {

    public DocumentationFacet documentation;
    public SourceCodeLocationFacet sourceCodeLocation;
    public SqlFacet sql;
    @Builder.Default
    private Map<String, Object> additional = new LinkedHashMap<>();

    @JsonAnySetter
    void setFacet(String key, Object value) {
      additional.put(key, value);
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalFacets() {
      return additional;
    }
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Builder
  public static class DocumentationFacet extends BaseFacet {
    @NonNull public String description;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Builder
  public static class SourceCodeLocationFacet extends BaseFacet {

    public String type;
    public String url;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Builder
  public static class SqlFacet extends BaseFacet {
    @NonNull public String query;
  }

  @ToString
  @NonNull
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Builder
  public static class LineageDataset {

    @NonNull public String namespace;
    @NonNull public String name;
    public DatasetFacet facets;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPropertyOrder({"documentation", "schema", "dataSource", "description"})
  @Builder
  public static class DatasetFacet {

    public DocumentationFacet documentation;
    public SchemaFacet schema;
    public DataSourceFacet dataSource;
    public String description;
    @Builder.Default
    private Map<String, Object> additional = new LinkedHashMap<>();

    @JsonAnySetter
    void setFacet(String key, Object value) {
      additional.put(key, value);
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalFacets() {
      return additional;
    }
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Builder
  public static class SchemaFacet extends BaseFacet {

    public List<SchemaField> fields;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Builder
  public static class SchemaField {

    @NonNull public String name;
    @NonNull public String type;
    public String description;
  }

  @ToString
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Builder
  public static class DataSourceFacet extends BaseFacet {

    public String name;
    public String uri;
  }
}
