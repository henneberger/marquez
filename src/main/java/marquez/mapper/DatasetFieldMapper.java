package marquez.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import marquez.db.Columns;
import marquez.graphql.Model.DatasetField;
import org.jdbi.v3.core.statement.StatementContext;

public class DatasetFieldMapper extends AbstractMapper<DatasetField> {
  @Override
  public DatasetField map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());
    return new DatasetField(
        uuidOrThrow(results, Columns.ROW_UUID, columnNames),
        stringOrThrow(results, Columns.TYPE, columnNames),
        timestampOrThrow(results, Columns.CREATED_AT, columnNames),
        timestampOrThrow(results, Columns.UPDATED_AT, columnNames),
        stringOrThrow(results, Columns.NAME, columnNames),
        Optional.ofNullable(stringOrNull(results, Columns.DESCRIPTION, columnNames)),
        null, //toDatasetLink(uuidOrThrow(results, Columns.DATASET_UUID, columnNames)),
        null,
        null);//toTagsLink(uuidArrayOrThrow(results, Columns.TAG_UUIDS, columnNames)));
  }
}