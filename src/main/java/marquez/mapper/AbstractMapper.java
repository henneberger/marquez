package marquez.mapper;

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
import marquez.graphql.Model.DatasetField;
import marquez.graphql.Model.DatasetVersion;
import marquez.graphql.Model.JobContext;
import marquez.graphql.Model.JobVersion;
import marquez.service.models.DbTable;
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

  public static List<UUID> uuidArrayOrNull(final ResultSet results, final String column, Set<String> columnNames)
      throws SQLException {
    if (!columnNames.contains(column) || results.getObject(column) == null) {
      return null;
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