package marquez.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import marquez.db.Columns;
import marquez.graphql.Model.Source;
import org.jdbi.v3.core.statement.StatementContext;

public final class SourceMapper extends AbstractMapper<Source> {
  @Override
  public Source map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());
    return new Source(
        uuidOrThrow(results, Columns.ROW_UUID, columnNames),
        stringOrThrow(results, Columns.TYPE, columnNames),
        stringOrThrow(results, Columns.NAME, columnNames),
        timestampOrThrow(results, Columns.CREATED_AT, columnNames),
        timestampOrThrow(results, Columns.UPDATED_AT, columnNames),
        stringOrThrow(results, Columns.CONNECTION_URL, columnNames),
        Optional.ofNullable(stringOrNull(results, Columns.DESCRIPTION, columnNames)),
        null);
  }
}
