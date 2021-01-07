package marquez.graphql;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

public class Model {
  @AllArgsConstructor

  @Getter
  @Setter
  @ToString
  public static class Tag {
    public Tag(UUID uuid) {
      this.uuid = uuid;
    }
    public final UUID uuid;
    public String name;
    public Instant createdAt;
    public Instant updatedAt;
    public Optional<String> description;

    public Set<DatasetField> fields;
    public Set<Dataset> datasets;
  }
  @AllArgsConstructor
  @NoArgsConstructor
  @Getter
  @Setter @ToString
  public static class Source {
    public Source(UUID uuid) {
      this.uuid = uuid;
    }
    public UUID uuid;
    public String type;
    public String name;
    public Instant createdAt;
    public Instant updatedAt;
    public String connectionUrl;
    public Optional<String> description;
    public List<Dataset> datasets;
  }
  @AllArgsConstructor
  @Getter
  @Setter @ToString
  public static class RunStateRecord {
    public RunStateRecord(UUID uuid) {
      this.uuid = uuid;
    }
    public final UUID uuid;
    public Instant transitionedAt;
    public String state;
    public Run run;
  }
  @AllArgsConstructor

  @Getter
  @Setter @ToString
  public static class RunArgs {
    public RunArgs(UUID uuid) {
      this.uuid = uuid;
    }
    public final UUID uuid;
    public Instant createdAt;
    public Map<String, String> args;
    public String checksum;
    public List<Run> run;
  }
  @AllArgsConstructor

  @Getter
  @Setter @ToString
  public static class Run {
    public Run(UUID uuid) {
      this.uuid = uuid;
    }
    public final UUID uuid;
    public Instant createdAt;
    public Instant updatedAt;
    public Optional<Instant> nominalStartTime;
    public Optional<Instant> nominalEndTime;

    public JobVersion jobVersion;
    public RunArgs runArgs;
    public String currentState;
    public RunStateRecord startState;
    public RunStateRecord endState;
    public List<DatasetVersion> inputs;
    public List<DatasetVersion> outputs;
  }
  @AllArgsConstructor

  @Getter
  @Setter @ToString
  public static class OwnerRecord {
    public OwnerRecord(UUID uuid) {
      this.uuid = uuid;
    }
    public final UUID uuid;
    public Instant startedAt;
    public Optional<Instant> endedAt;

    public Owner owner;
    public Namespace namespace;
  }
  @AllArgsConstructor

  @Getter
  @Setter @ToString
  public static class Owner {
    public Owner(UUID uuid) {
      this.uuid = uuid;
    }    public Owner(String name) {
      this.name = name;
    }
    public UUID uuid;
    public Instant createdAt;
    public String name;
    public List<OwnerRecord> record;
  }
  @AllArgsConstructor

  @Getter
  @Setter @ToString
  public static class Namespace {
    public Namespace(UUID uuid) {
      this.uuid = uuid;
    }
    public final UUID uuid;
    public String name;
    public Instant createdAt;
    public Instant updatedAt;
    public Optional<String> description;
    public List<OwnerRecord> ownerRecord;
    public Owner currentOwner;
  }
  @AllArgsConstructor

  @Getter
  @Setter @ToString
  public static class JobVersion {
    public JobVersion(UUID uuid) {
      this.uuid = uuid;
    }
    public final UUID uuid;
    public Instant createdAt;
    public Instant updatedAt;
    public Optional<String> location;
    public UUID version;
    public JobContext jobContext;
    public Optional<Run> latestRun;
    public Job job;
    public List<Dataset> inputs;
    public List<Dataset> outputs;
  }
  @Getter
  @AllArgsConstructor

  public static class JobContext {
    public JobContext(UUID uuid) {
      this.uuid = uuid;
    }
    public final UUID uuid;
    public Instant createdAt;
    public String context;
    public String checksum;
    public JobVersion jobVersion;
  }
  @AllArgsConstructor

  @Getter
  @Setter @ToString
  public static class Job {
    public Job(UUID uuid) {
      this.uuid = uuid;
    }
    public final UUID uuid;
    public String type;
    public String name;
    public Instant createdAt;
    public Instant updatedAt;
    public Optional<String> description;
    public List<JobVersion> versions;
    public Namespace namespace;
    public JobVersion currentVersion;
  }
  @AllArgsConstructor
  @Getter
  @Setter @ToString
  public static class DatasetVersion {
    public DatasetVersion(UUID uuid) {
      this.uuid = uuid;
    }
    public final UUID uuid;
    public Instant createdAt;
    public Dataset dataset;
    public UUID version;
    public List<DatasetField> fields;
    public Optional<Run> run;
  }
  @AllArgsConstructor

  @Getter
  @Setter @ToString
  public static class DatasetField {
    public DatasetField(UUID uuid) {
      this.uuid = uuid;
    }
    public final UUID uuid;
    public String type;
    public Instant createdAt;
    public Instant updatedAt;
    public String name;
    public Optional<String> description;
    public Dataset dataset;
    public List<DatasetVersion> versions;
    public List<Tag> tags;
  }
  @AllArgsConstructor
  @Getter
  @Setter
  @ToString
  public static class Dataset {
    public Dataset(UUID uuid) {
      this.uuid = uuid;
    }
    public final UUID uuid;
    public String type;
    public String name;
    public String physicalName;
    public Instant createdAt;
    public Instant updatedAt;
    public Optional<Instant> lastModifiedAt;
    public Optional<String> description;
    public Source source;
    public List<DatasetField> fields;
    public List<JobVersion> jobVersionAsInput;
    public List<JobVersion> jobVersionAsOutput;
    public Namespace namespace;
    public List<Tag> tags;
    public DatasetVersion currentVersion;
    public List<DatasetVersion> versions;
  }
  public enum RunState {
    NEW,
    RUNNING,
    COMPLETED,
    ABORTED,
    FAILED;
  }
}
