package marquez.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import marquez.db.Columns;
import marquez.graphql.Model.Tag;
import org.jdbi.v3.core.statement.StatementContext;

public final class TagMapper extends AbstractMapper<Tag> {
  @Override
  public Tag map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());
    return new Tag(
        uuidOrThrow(results, Columns.ROW_UUID, columnNames),
        stringOrThrow(results, Columns.NAME, columnNames),
        timestampOrThrow(results, Columns.CREATED_AT, columnNames),
        timestampOrThrow(results, Columns.UPDATED_AT, columnNames),
        Optional.ofNullable(stringOrNull(results, Columns.DESCRIPTION, columnNames)),
        null, null);
  }
}