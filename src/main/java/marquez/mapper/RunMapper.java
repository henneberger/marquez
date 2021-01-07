package marquez.mapper;

import com.fasterxml.jackson.core.type.TypeReference;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.Utils;
import marquez.db.Columns;
import marquez.graphql.Model;
import marquez.graphql.Model.JobVersion;
import marquez.graphql.Model.Run;
import marquez.graphql.Model.RunArgs;
import marquez.graphql.Model.RunStateRecord;
import org.jdbi.v3.core.statement.StatementContext;

@Slf4j
public final class RunMapper extends AbstractMapper<Run> {
  @Override
  public Run map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());

    return new Run(
        uuidOrThrow(results, Columns.ROW_UUID, columnNames),
        timestampOrThrow(results, Columns.CREATED_AT, columnNames),
        timestampOrThrow(results, Columns.UPDATED_AT, columnNames),
        Optional.ofNullable(timestampOrNull(results, Columns.NOMINAL_START_TIME, columnNames)),
        Optional.ofNullable(timestampOrNull(results, Columns.NOMINAL_END_TIME, columnNames)),
        new JobVersion(uuidOrNull(results, Columns.JOB_VERSION_UUID, columnNames)),
        new RunArgs(uuidOrNull(results, Columns.RUN_ARGS_UUID, columnNames)),
        stringOrNull(results, Columns.CURRENT_RUN_STATE, columnNames),
        new RunStateRecord(uuidOrNull(results, Columns.START_RUN_STATE_UUID, columnNames)),
        new RunStateRecord(uuidOrNull(results, Columns.END_RUN_STATE_UUID, columnNames)),
        null,//toDatasetVersionsLink(uuidArrayOrThrow(results, Columns.INPUT_VERSION_UUIDS, columnNames)),
        null
    );
  }
}