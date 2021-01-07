package marquez.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import marquez.db.Columns;
import marquez.graphql.Model;
import marquez.graphql.Model.Job;
import marquez.graphql.Model.JobVersion;
import marquez.graphql.Model.Namespace;
import org.jdbi.v3.core.statement.StatementContext;

public final class JobMapper extends AbstractMapper<Job> {
  @Override
  public Job map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());
    return new Job(
        uuidOrThrow(results, Columns.ROW_UUID, columnNames),
        stringOrThrow(results, Columns.TYPE, columnNames),
        stringOrThrow(results, Columns.NAME, columnNames),
        timestampOrThrow(results, Columns.CREATED_AT, columnNames),
        timestampOrThrow(results, Columns.UPDATED_AT, columnNames),
        Optional.ofNullable(stringOrNull(results, Columns.DESCRIPTION, columnNames)),
        null,
        new Namespace(uuidOrThrow(results, Columns.NAMESPACE_UUID, columnNames)),
        new JobVersion(uuidOrNull(results, Columns.CURRENT_VERSION_UUID, columnNames)));
  }
}