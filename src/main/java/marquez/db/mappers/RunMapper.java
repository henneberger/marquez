/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package marquez.db.mappers;

import static marquez.db.Columns.RUN_ARGS_UUID;
import static marquez.db.Columns.stringOrNull;
import static marquez.db.Columns.timestampOrNull;
import static marquez.db.Columns.timestampOrThrow;
import static marquez.db.Columns.uuidArrayOrThrow;
import static marquez.db.Columns.uuidOrNull;
import static marquez.db.Columns.uuidOrThrow;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableSet;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.Utils;
import marquez.common.models.RunId;
import marquez.common.models.RunState;
import marquez.db.Columns;
import marquez.db.models.DatasetVersionRow;
import marquez.db.models.JobVersionRow;
import marquez.db.models.RunArgsRow;
import marquez.service.models.Run;
import marquez.service.models.RunStateRow;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

@Slf4j
public final class RunMapper implements RowMapper<Run> {
  @Override
  public Run map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());
    Run run =
        new Run(
          RunId.of(uuidOrThrow(results, Columns.ROW_UUID)),
          timestampOrThrow(results, Columns.CREATED_AT),
          timestampOrThrow(results, Columns.UPDATED_AT),
          null,
          null,
            Optional.ofNullable(timestampOrNull(results, Columns.NOMINAL_START_TIME)),
            Optional.ofNullable(timestampOrNull(results, Columns.NOMINAL_END_TIME)),
          RunStateRow.builder()
              .state(stringOrNull(results, Columns.CURRENT_RUN_STATE) == null ?
                  null : RunState.valueOf(stringOrNull(results, Columns.CURRENT_RUN_STATE)))
              .build(),
          null,
          null,
            null,
            null
        );

    if (columnNames.contains(Columns.JOB_VERSION_UUID) && results.getObject(Columns.JOB_VERSION_UUID) != null) {
      run.jobVersion = JobVersionRow.builder()
          .uuid(uuidOrNull(results, Columns.JOB_VERSION_UUID))
          .build();
    }
    if (columnNames.contains(Columns.RUN_ARGS_UUID)
        && results.getObject(Columns.RUN_ARGS_UUID) != null) {
      RunArgsRow.RunArgsRowBuilder builder = RunArgsRow.builder()
          .uuid(uuidOrNull(results, Columns.RUN_ARGS_UUID));
      if (columnNames.contains(Columns.ARGS))
        builder.args(Utils.fromJson(stringOrNull(results, Columns.ARGS), new TypeReference<>() {}));
      run.runArgs = builder.build();
    }
    if (columnNames.contains(Columns.INPUT_VERSION_UUIDS)
        && results.getObject(Columns.INPUT_VERSION_UUIDS) != null) {
      List<UUID> inputs = uuidArrayOrThrow(results, Columns.INPUT_VERSION_UUIDS);
      run.inputs = inputs.stream()
          .map(dv -> DatasetVersionRow.builder()
              .uuid(dv)
              .build())
          .collect(Collectors.toList());
    }
    if (columnNames.contains(Columns.START_RUN_STATE_UUID)
        && results.getObject(Columns.START_RUN_STATE_UUID) != null) {
      RunStateRow.RunStateRowBuilder builder = RunStateRow.builder()
          .uuid(uuidOrNull(results, Columns.START_RUN_STATE_UUID));
      if (columnNames.contains(Columns.STARTED_AT)) {
        builder.transitionedAt(timestampOrNull(results, Columns.STARTED_AT));
      }
      run.startState = builder.build();
    }
    if (columnNames.contains(Columns.END_RUN_STATE_UUID)
        && results.getObject(Columns.END_RUN_STATE_UUID) != null) {
      RunStateRow.RunStateRowBuilder builder = RunStateRow.builder()
          .uuid(uuidOrNull(results, Columns.END_RUN_STATE_UUID));
      if (columnNames.contains(Columns.ENDED_AT)) {
        builder.transitionedAt(timestampOrNull(results, Columns.ENDED_AT));
      }
      run.endState = builder.build();
    }
    return run;
  }

  private Set<String> getColumnNames(ResultSetMetaData metaData) {
    try {
      Set<String> columns = new HashSet<>();
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        columns.add(metaData.getColumnName(i));
      }
      return columns;
    } catch (SQLException e) {
      log.error("Unable to get column names", e);
    }
    return ImmutableSet.of();
  }
}
