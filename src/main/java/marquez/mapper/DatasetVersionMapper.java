package marquez.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import marquez.db.Columns;
import marquez.graphql.Model;
import marquez.graphql.Model.Dataset;
import marquez.graphql.Model.DatasetField;
import marquez.graphql.Model.DatasetVersion;
import marquez.graphql.Model.Run;
import org.jdbi.v3.core.statement.StatementContext;

public final class DatasetVersionMapper extends AbstractMapper<DatasetVersion> {
  @Override
  public DatasetVersion map(
      @NonNull ResultSet results, @NonNull StatementContext context) throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());

    return new DatasetVersion(
        uuidOrThrow(results, Columns.ROW_UUID, columnNames),
        timestampOrThrow(results, Columns.CREATED_AT, columnNames),
        new Dataset(uuidOrThrow(results, Columns.DATASET_UUID, columnNames)),
        uuidOrThrow(results, Columns.VERSION, columnNames),
        null,
        uuidOrNull(results, Columns.RUN_UUID, columnNames) == null ? Optional.empty() : Optional.of(new Run(uuidOrNull(results, Columns.RUN_UUID, columnNames)))
      );
  }

}