package marquez.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import lombok.NonNull;
import marquez.db.Columns;
import marquez.graphql.Model.Run;
import marquez.graphql.Model.RunStateRecord;
import org.jdbi.v3.core.statement.StatementContext;

public final class RunStateRecordMapper extends AbstractMapper<RunStateRecord> {
  @Override
  public RunStateRecord map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());
    return new RunStateRecord(
        uuidOrThrow(results, Columns.ROW_UUID, columnNames),
        timestampOrThrow(results, Columns.TRANSITIONED_AT, columnNames),
        stringOrNull(results, Columns.STATE, columnNames),
        new Run(uuidOrThrow(results, Columns.RUN_UUID, columnNames))
    );
  }
}