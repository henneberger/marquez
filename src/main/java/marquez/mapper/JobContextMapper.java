package marquez.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import lombok.NonNull;
import marquez.db.Columns;
import marquez.graphql.Model.JobContext;
import org.jdbi.v3.core.statement.StatementContext;

public final class JobContextMapper extends AbstractMapper<JobContext> {
  @Override
  public JobContext map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());

    return new JobContext(
        uuidOrThrow(results, Columns.ROW_UUID, columnNames),
        timestampOrThrow(results, Columns.CREATED_AT, columnNames),
        stringOrThrow(results, Columns.CONTEXT, columnNames),
        stringOrThrow(results, Columns.CHECKSUM, columnNames),
        null);
  }
}