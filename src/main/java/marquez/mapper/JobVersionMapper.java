package marquez.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import marquez.db.Columns;
import marquez.graphql.Model;
import marquez.graphql.Model.Job;
import marquez.graphql.Model.JobContext;
import marquez.graphql.Model.JobVersion;
import marquez.graphql.Model.Run;
import org.jdbi.v3.core.statement.StatementContext;

public final class JobVersionMapper extends AbstractMapper<JobVersion> {
  @Override
  public JobVersion map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());
    return new JobVersion(
        uuidOrThrow(results, Columns.ROW_UUID, columnNames),
        timestampOrThrow(results, Columns.CREATED_AT, columnNames),
        timestampOrThrow(results, Columns.UPDATED_AT, columnNames),
        Optional.ofNullable(stringOrNull(results, Columns.LOCATION, columnNames)),
        uuidOrThrow(results, Columns.VERSION, columnNames),
        new JobContext(uuidOrThrow(results, Columns.JOB_CONTEXT_UUID, columnNames)),
        Optional.ofNullable(uuidOrNull(results, Columns.LATEST_RUN_UUID, columnNames) == null ? null : new Run(uuidOrNull(results, Columns.LATEST_RUN_UUID, columnNames))),
        new Job(uuidOrThrow(results, Columns.JOB_UUID, columnNames)),
            null,//    toNamespaceLink(uuidOrThrow(results, Columns.NAMESPACE_UUID, columnNames), stringOrThrow(results, Columns.NAMESPACE_NAME, columnNames))),
        null//toDatasetsLink(uuidArrayOrThrow(results, Columns.INPUT_UUIDS, columnNames)),
       //toDatasetsLink(uuidArrayOrThrow(results, Columns.OUTPUT_UUIDS, columnNames))
    );
  }
}