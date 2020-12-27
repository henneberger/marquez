package marquez.db.mappers;

import com.google.common.collect.ImmutableSet;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import marquez.service.models.Dataset;
import marquez.service.models.DatasetField;
import marquez.service.models.DatasetVersion;
import marquez.service.models.DbTable;
import marquez.service.models.Job;
import marquez.service.models.JobContext;
import marquez.service.models.JobVersion;
import marquez.service.models.Namespace;
import marquez.service.models.Owner;
import marquez.service.models.Run;
import marquez.service.models.Source;
import marquez.service.models.Tag;
import org.jdbi.v3.core.mapper.RowMapper;
import org.postgresql.util.PGInterval;

@Slf4j
public abstract class AbstractMapper<T> implements RowMapper<T> {

  protected Set<String> getColumnNames(ResultSetMetaData metaData) {
    try {
      Set<String> columns = new HashSet<>();
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        columns.add(metaData.getColumnName(i));
      }
      return columns;
    } catch (SQLException e) {
      log.error("Unable to get column names", e);
    }
    return ImmutableSet.of();
  }

  protected boolean hasObject(ResultSet results, Set<String> columnNames,
      String column) throws SQLException {
    return columnNames.contains(column) && results.getObject(column) != null;
  }

  protected Source toSourceLink(UUID sourceUuid, String name) {
    return Source.builder()
        .uuid(sourceUuid)
        .name(name)
        .build();
  }

  protected Dataset toDatasetLink(UUID datasetUuid) {
    return DbTable.builder()
        .uuid(datasetUuid)
        .build();
  }

  protected Dataset toDatasetLink(UUID datasetUuid, String name) {
    return Dataset.builder()
        .uuid(datasetUuid)
        .name(name)
        .build();
  }
  protected List<Tag> toTagsLink(List<UUID> tagUuids) {
    return tagUuids.stream()
        .map(t-> Tag.builder().uuid(t).build())
        .collect(Collectors.toList());
  }

  protected DatasetVersion toDatasetVersionLink(UUID uuid) {
    if (uuid == null) {
      return null;
    }
    return DatasetVersion.builder()
        .uuid(uuid).build();
  }

  protected Namespace toNamespaceLink(UUID namespaceUuid, String name) {
    if (namespaceUuid == null) {
      return null;
    }
    return Namespace.builder()
        .uuid(namespaceUuid)
        .name(name)
        .build();
  }

  protected Optional<Run> toRunLink(UUID runUuid) {
    if (runUuid == null) {
      return Optional.empty();
    }
    return Optional.of(Run.builder()
        .uuid(runUuid)
        .build());
  }

  protected List<DatasetField> toDatasetFieldsLink(List<UUID> fields) {
    return fields.stream()
        .map(f->DatasetField.builder()
            .uuid(f)
            .build())
        .collect(Collectors.toList());
  }

  protected JobVersion toJobVersionLink(UUID uuid) {
    if (uuid == null) {
      return null;
    }
    return JobVersion.builder()
        .uuid(uuid).build();
  }

  protected JobContext toJobContextLink(UUID uuid, String context) {
    return JobContext.builder()
        .uuid(uuid)
        .context(context)
        .build();
  }

  protected Job toJobLink(UUID jobUuid, String jobName, Namespace namespace) {
    return Job.builder()
        .uuid(jobUuid)
        .name(jobName)
        .namespace(namespace)
        .build();
  }

  protected List<Dataset> toDatasetsLink(List<UUID> datasets) {
    return datasets.stream()
        .map(this::toDatasetLink)
        .collect(Collectors.toList());
  }
  protected List<DatasetVersion> toDatasetVersionsLink(List<UUID> inputs) throws SQLException {
    return inputs.stream()
        .map(this::toDatasetVersionLink)
        .collect(Collectors.toList());
  }
  protected Owner toOwnerLink(String name) {
    if (name == null) {
      return null;
    }
    return Owner.builder()
        .name(name)
        .build();
  }
  public static UUID uuidOrNull(final ResultSet results, final String column, Set<String> columnNames) throws SQLException {
    if (!columnNames.contains(column) || results.getObject(column) == null) {
      return null;
    }
    return results.getObject(column, UUID.class);
  }

  public static UUID uuidOrThrow(final ResultSet results, final String column, Set<String> columnNames) throws SQLException {
    if (results.getObject(column) == null) {
      throw new IllegalArgumentException();
    }
    return results.getObject(column, UUID.class);
  }

  public static Instant timestampOrNull(final ResultSet results, final String column, Set<String> columnNames)
      throws SQLException {
    if (!columnNames.contains(column) || results.getObject(column) == null) {
      return null;
    }
    return results.getTimestamp(column).toInstant();
  }

  public static Instant timestampOrThrow(final ResultSet results, final String column, Set<String> columnNames)
      throws SQLException {
    if (!columnNames.contains(column) || results.getObject(column) == null) {
      throw new IllegalArgumentException();
    }
    return results.getTimestamp(column).toInstant();
  }

  public static String stringOrNull(final ResultSet results, final String column, Set<String> columnNames)
      throws SQLException {
    if (!columnNames.contains(column) || results.getObject(column) == null) {
      return null;
    }
    return results.getString(column);
  }

  public static String stringOrThrow(final ResultSet results, final String column, Set<String> columnNames)
      throws SQLException {
    if (!columnNames.contains(column) || results.getObject(column) == null) {
      throw new IllegalArgumentException();
    }
    return results.getString(column);
  }

  public static int intOrThrow(final ResultSet results, final String column, Set<String> columnNames) throws SQLException {
    if (!columnNames.contains(column) || results.getObject(column) == null) {
      throw new IllegalArgumentException();
    }
    return results.getInt(column);
  }

  public static PGInterval pgIntervalOrThrow(final ResultSet results, final String column, Set<String> columnNames)
      throws SQLException {
    if (!columnNames.contains(column) || results.getObject(column) == null) {
      throw new IllegalArgumentException();
    }
    return new PGInterval(results.getString(column));
  }

  public static BigDecimal bigDecimalOrThrow(final ResultSet results, final String column, Set<String> columnNames)
      throws SQLException {
    if (!columnNames.contains(column) || results.getObject(column) == null) {
      throw new IllegalArgumentException();
    }
    return results.getBigDecimal(column);
  }

  public static List<UUID> uuidArrayOrThrow(final ResultSet results, final String column, Set<String> columnNames)
      throws SQLException {
    if (!columnNames.contains(column) || results.getObject(column) == null) {
      throw new IllegalArgumentException();
    }
    return Arrays.asList((UUID[]) results.getArray(column).getArray());
  }

  public static List<String> stringArrayOrThrow(final ResultSet results, final String column, Set<String> columnNames)
      throws SQLException {
    if (!columnNames.contains(column) || results.getObject(column) == null) {
      throw new IllegalArgumentException();
    }
    return Arrays.asList((String[]) results.getArray(column).getArray());
  }
}
