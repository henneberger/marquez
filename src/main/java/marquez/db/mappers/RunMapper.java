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

import com.fasterxml.jackson.core.type.TypeReference;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.Utils;
import marquez.common.models.RunState;
import marquez.db.Columns;
import marquez.service.models.Run;
import marquez.service.models.RunArgs;
import marquez.service.models.RunArgs.RunArgsBuilder;
import marquez.service.models.RunStateRecord;
import marquez.service.models.RunStateRecord.RunStateRecordBuilder;
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
        toJobVersionLink(uuidOrNull(results, Columns.JOB_VERSION_UUID, columnNames)),
        getRunArgs(results, columnNames),
        getCurrentState(results, columnNames),
        getState(results, columnNames, Columns.START_RUN_STATE_UUID, Columns.STARTED_AT),
        getState(results, columnNames, Columns.END_RUN_STATE_UUID, Columns.ENDED_AT),
        toDatasetVersionsLink(uuidArrayOrThrow(results, Columns.INPUT_VERSION_UUIDS, columnNames)),
        null
    );
  }

  private RunStateRecord getCurrentState(ResultSet results, Set<String> columnNames) throws SQLException {
    RunStateRecord currentState = RunStateRecord.builder()
        .state(stringOrNull(results, Columns.CURRENT_RUN_STATE, columnNames) == null ?
            null : RunState.valueOf(stringOrNull(results, Columns.CURRENT_RUN_STATE, columnNames)))
        .build();
    return currentState;
  }

  private RunArgs getRunArgs(ResultSet results, Set<String> columnNames) throws SQLException {
    if (hasObject(results, columnNames, Columns.RUN_ARGS_UUID)) {
      RunArgsBuilder builder = RunArgs.builder()
          .uuid(uuidOrNull(results, Columns.RUN_ARGS_UUID, columnNames));
      if (hasObject(results, columnNames, Columns.ARGS)) {
        builder.args(Utils.fromJson(stringOrNull(results, Columns.ARGS, columnNames), new TypeReference<>() {
        }));
      }
      return builder.build();
    }
    return null;
  }

  private RunStateRecord getState(@NonNull ResultSet results, Set<String> columnNames,
      String endRunStateUuid, String endedAt) throws SQLException {
    if (hasObject(results, columnNames, endRunStateUuid)) {
      RunStateRecordBuilder builder = RunStateRecord.builder()
          .uuid(uuidOrNull(results, endRunStateUuid, columnNames));
      if (columnNames.contains(endedAt)) {
        builder.transitionedAt(timestampOrNull(results, endedAt, columnNames));
      }
      return builder.build();
    }
    return null;
  }
}
