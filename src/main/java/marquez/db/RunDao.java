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

package marquez.db;

import static marquez.db.Columns.ENDED_AT;
import static marquez.db.Columns.STARTED_AT;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import marquez.db.mappers.DatasetVersionMapper;
import marquez.db.mappers.RunMapper;
import marquez.service.input.RunInsertFragment;
import marquez.service.input.RunStateInsertFragment;
import marquez.service.models.DatasetVersion;
import marquez.service.models.JobVersion;
import marquez.service.models.Run;
import org.jdbi.v3.sqlobject.CreateSqlObject;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public interface RunDao extends SqlObject {
  @CreateSqlObject
  JobVersionDao createJobVersionDao();
  @CreateSqlObject
  DatasetVersionDao createDatasetVersionDao();

  default Run create(RunInsertFragment fragment) {
    return withHandle(c->
        c.inTransaction(handle -> {
          UUID runArgsUuid = null;
          if (fragment.getRunArgs() != null) {
            runArgsUuid = handle
                .createQuery("INSERT INTO run_args (created_at, args, checksum) "
                    + "VALUES (:createdAt, :args, :checksum) ON CONFLICT(checksum) DO UPDATE SET "
                    + "args = :args RETURNING uuid")
                .bind("createdAt", fragment.getRunArgs().getCreatedAt())
                .bind("args", fragment.getRunArgs().getArgs())
                .bind("checksum", fragment.getRunArgs().getChecksum())
                .mapTo(UUID.class)
                .one();
          }

          String runQuery = "INSERT INTO runs ("
              + "created_at, "
              + "updated_at, "
              + "job_version_uuid, "
              + "run_args_uuid, "
              + "nominal_start_time, "
              + "nominal_end_time,"
              + "current_run_state"
              + ") VALUES ("
              + ":createdAt, "
              + ":updatedAt, "
              + ":jobVersion.uuid, "
              + ":runArgsUuid, "
              + ":nominalStartTime, "
              + ":nominalEndTime,"
              + ":runState.state"
              + ") RETURNING uuid";

          UUID runUuid = handle.createQuery(runQuery)
              .bindBean(fragment)
              .bind("runArgsUuid", runArgsUuid)
              .mapTo(UUID.class)
              .one();

          String runState = "INSERT INTO run_states (transitioned_at, run_uuid, state)"
              + "VALUES (:transitionedAt, :runUuid, :state) RETURNING uuid";

          UUID runStateUuid = handle.createQuery(runState)
              .bindBean(fragment.getRunState())
              .bind("runUuid", runUuid)
              .mapTo(UUID.class)
              .one();
//
//          //Circular reference chain so we must perform an update
//          String updateState = "UPDATE runs "
//              + "SET updated_at = :updatedAt, "
//              + "    start_run_state_uuid = :startRunStateUuid "
//              + "WHERE uuid = :uuid";
//
//          handle.createUpdate(updateState)
//              .bind("updatedAt", Instant.now())
//              .bind("startRunStateUuid", runStateUuid)
//              .bind("uuid", runUuid)
//              .execute();

          return findBy(runUuid).get();
        }));
  }

  default Optional<Run> createState(RunStateInsertFragment fragment, Function<UUID, Optional<Run>> retriever) {
    UUID runUuid = withHandle(h -> h.inTransaction(handle-> {
      //1. Insert into run_state
      UUID runStateUuid = handle
          .createQuery("INSERT INTO run_states (transitioned_at, run_uuid, state)"
              + "VALUES (:transitionedAt, :runId, :state) returning uuid")
          .bindBean(fragment)
          .mapTo(UUID.class)
          .one();

      //2. Update run w/ denormalized run state
      StringBuilder updateRun = new StringBuilder("UPDATE runs SET current_run_state = :state, "
          + "updated_at = :updatedAt");
      /*Note: there is a legacy bug of repeated RUNNING states will update the started date to the most recent*/
      switch (fragment.getState()) {
        case NEW:
          break;
        case RUNNING:
          updateRun.append(", start_run_state_uuid = :currentStateUuid");
          break;
        case COMPLETED:
        case ABORTED:
        case FAILED:
          updateRun.append(", end_run_state_uuid = :currentStateUuid");
          break;
      }

      updateRun.append(" where uuid = :uuid RETURNING uuid");

      return handle
          .createQuery(updateRun.toString())
          .bindBean(fragment)
          .bind("currentStateUuid", runStateUuid)
          .bind("updatedAt", fragment.getTransitionedAt())
          .bind("uuid", fragment.getRunId())
          .mapTo(UUID.class)
          .one();

      //TODO: Is this valid? What does the last modified date signify?
      // Modified
//      if (complete && outputVersionUuids != null && outputVersionUuids.size() > 0) {
//        createDatasetDao().updateLastModifiedAt(outputVersionUuids, updateAt);
//      }
    }));

    return retriever.apply(runUuid);
  }


  @SqlQuery("SELECT EXISTS (SELECT 1 FROM runs WHERE uuid = :rowUuid)")
  boolean exists(UUID rowUuid);

  //todo: how to update this table?
  @SqlUpdate(
      "INSERT INTO runs_input_mapping (run_uuid, dataset_version_uuid) "
          + "VALUES (:runUuid, :datasetVersionUuid) ON CONFLICT DO NOTHING")
  void updateInputVersions(UUID runUuid, UUID datasetVersionUuid);

  static final String SELECT_RUN =
      "SELECT r.*, ra.args, rs_s.transitioned_at as "
          + STARTED_AT
          + ", rs_e.transitioned_at as "
          + ENDED_AT
          + ", "
          + "ARRAY(SELECT dataset_version_uuid "
          + "      FROM runs_input_mapping "
          + "      WHERE run_uuid = r.uuid) AS input_version_uuids "
          + "FROM runs AS r "
          + "LEFT JOIN run_args AS ra"
          + "  ON (ra.uuid = r.run_args_uuid) "
          + "LEFT JOIN run_states AS rs_s"
          + "  ON (rs_s.uuid = r.start_run_state_uuid) " //todo: this is also likely not right, should be current state
          + "LEFT JOIN run_states AS rs_e"
          + "  ON (rs_e.uuid = r.end_run_state_uuid) ";

  @SqlQuery(SELECT_RUN + " WHERE r.uuid = :rowUuid")
  @RegisterRowMapper(RunMapper.class)
  Optional<Run> findBy(UUID rowUuid);

  default Optional<Run> findByWithDatasets(UUID runUuid) {
    return withHandle(handle-> {
      Optional<Run> run = handle.createQuery(SELECT_RUN + " WHERE r.uuid = :rowUuid")
          .bind("rowUuid", runUuid)
          .map(new RunMapper())
          .findOne();
      if (run.isEmpty()) {
        return run;
      }

      JobVersion jobVersion = createJobVersionDao()
          .findBy(run.get().getJobVersion().getUuid())
          .orElseThrow();
      run.get().setJobVersion(jobVersion);

      List<DatasetVersion> outputs = createDatasetVersionDao()
          .findByRunId(run.get().getUuid());
      run.get().setOutputs(outputs);

      List<DatasetVersion> inputs = handle.createQuery(
          "SELECT dv.*, d.name as dataset_name, n.name as namespace_name, n.uuid as namespace_uuid, "
              + "ARRAY(SELECT dataset_field_uuid "
              + "      FROM dataset_versions_field_mapping "
              + "      WHERE dataset_version_uuid = dv.uuid) AS field_uuids "
              + " FROM runs_input_mapping i"
              + " INNER JOIN dataset_versions dv on i.dataset_version_uuid = dv.uuid"
              + " INNER JOIN datasets AS d ON d.uuid = dv.dataset_uuid "
              + " INNER JOIN namespaces AS n ON n.uuid = d.namespace_uuid "
              + " WHERE i.run_uuid = :runUuid")
          .bind("runUuid", runUuid)
          .map(new DatasetVersionMapper())
          .list();
      run.get().setInputs(inputs);

      return run;
    });
  }

  @SqlQuery(
      SELECT_RUN
          + "INNER JOIN job_versions AS jv ON r.job_version_uuid = jv.uuid "
          + "INNER JOIN jobs AS j ON jv.job_uuid = j.uuid "
          + "INNER JOIN namespaces AS n ON j.namespace_uuid = n.uuid "
          + "WHERE n.name = :namespace and j.name = :jobName "
          + "ORDER BY r.created_at DESC "
          + "LIMIT :limit OFFSET :offset")
  @RegisterRowMapper(RunMapper.class)
  List<Run> findAll(String namespace, String jobName, int limit, int offset);

  @SqlQuery("SELECT COUNT(*) FROM runs")
  int count();
}
