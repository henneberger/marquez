package marquez.mapper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import marquez.common.Utils;
import marquez.db.Columns;
import marquez.graphql.Model;
import marquez.graphql.Model.DatasetVersion;
import marquez.graphql.Model.RunArgs;
import org.jdbi.v3.core.statement.StatementContext;

public final class RunArgsMapper extends AbstractMapper<RunArgs> {
  @Override
  public RunArgs map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());
    return new RunArgs(
        uuidOrThrow(results, Columns.ROW_UUID, columnNames),
        timestampOrThrow(results, Columns.CREATED_AT, columnNames),
        Utils.fromJson(stringOrNull(results, Columns.ARGS, columnNames), new TypeReference<ImmutableMap<String, String>>() {}),
        stringOrNull(results, Columns.CHECKSUM, columnNames),
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