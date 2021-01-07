package marquez.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import marquez.db.Columns;
import marquez.graphql.Model;
import marquez.graphql.Model.DatasetVersion;
import marquez.graphql.Model.Owner;
import org.jdbi.v3.core.statement.StatementContext;

public final class OwnerMapper extends AbstractMapper<Owner> {
  @Override
  public Owner map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());
    return new Owner(
        uuidOrThrow(results, Columns.ROW_UUID, columnNames),
        timestampOrNull(results, Columns.CREATED_AT, columnNames),
        stringOrNull(results, Columns.NAME, columnNames),
        null

    );
  }
}