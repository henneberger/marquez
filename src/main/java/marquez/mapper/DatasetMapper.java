package marquez.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import marquez.db.Columns;
import marquez.graphql.Model;
import marquez.graphql.Model.Dataset;
import marquez.graphql.Model.DatasetVersion;
import marquez.graphql.Model.Namespace;
import org.jdbi.v3.core.statement.StatementContext;

public final class DatasetMapper extends AbstractMapper<Dataset> {
  @Override
  public Dataset map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());
    String type = stringOrThrow(results, Columns.TYPE, columnNames);
    return new Dataset(
        uuidOrThrow(results, Columns.ROW_UUID, columnNames),
        type,
        stringOrThrow(results, Columns.NAME, columnNames),
        stringOrThrow(results, Columns.PHYSICAL_NAME, columnNames),
        timestampOrThrow(results, Columns.CREATED_AT, columnNames),
        timestampOrThrow(results, Columns.UPDATED_AT, columnNames),
        Optional.ofNullable(timestampOrNull(results, Columns.LAST_MODIFIED_AT, columnNames)),
        Optional.ofNullable(stringOrNull(results, Columns.DESCRIPTION, columnNames)),
        new Model.Source(uuidOrThrow(results, Columns.SOURCE_UUID, columnNames)),
        null,
        null,
        null,
        new Namespace(uuidOrThrow(results, Columns.NAMESPACE_UUID, columnNames)),
        null,
        new DatasetVersion(uuidOrNull(results, Columns.CURRENT_VERSION_UUID, columnNames)),
        null
    );
//        toSourceLink(uuidOrThrow(results, Columns.SOURCE_UUID, columnNames), stringOrThrow(results, Columns.SOURCE_NAME, columnNames)),
//        null,
//        null,
//        null,
//        toNamespaceLink(uuidOrThrow(results, Columns.NAMESPACE_UUID, columnNames), stringOrThrow(results, Columns.NAMESPACE_NAME, columnNames)),
//        toTagsLink(uuidArrayOrThrow(results, Columns.TAG_UUIDS, columnNames)),
//        toDatasetVersionLink(uuidOrNull(results, Columns.CURRENT_VERSION_UUID, columnNames)));
  }
}