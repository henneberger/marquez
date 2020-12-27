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

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import marquez.common.Utils;
import marquez.db.JobVersionDao.IoType;
import marquez.db.mappers.JobMapper;
import marquez.service.input.JobInsertFragment;
import marquez.service.input.JobServiceFragment.DatasetFragment;
import marquez.service.models.Dataset;
import marquez.service.models.Job;
import marquez.service.models.Namespace;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.jdbi.v3.sqlobject.CreateSqlObject;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

@RegisterRowMapper(JobMapper.class)
public interface JobDao extends SqlObject {
  @CreateSqlObject
  NamespaceDao createNamespaceDao();
  @CreateSqlObject
  DatasetDao createDatasetDao();

  default Job create(JobInsertFragment fragment) {
    UUID job = withHandle(t -> t.inTransaction(handle -> {
      Instant now = Instant.now();
      Namespace namespace = createNamespaceDao().findBy(fragment.getNamespace()).get();

      UUID jobUuid = handle.createQuery("INSERT INTO jobs ("
          + "type, "
          + "created_at, "
          + "updated_at, "
          + "namespace_uuid, "
          + "name, "
          + "description"
          + ") VALUES ("
          + ":type, "
          + ":createdAt, "
          + ":updatedAt, "
          + ":namespaceUuid, "
          + ":name, "
          + ":description) ON CONFLICT (name, namespace_uuid) DO UPDATE" //todo: Description is optional
          + " SET updated_at = :updatedAt, type = :type, description = :description"
          + " RETURNING uuid")
          .bind("type", fragment.getType())
          .bind("createdAt", now)
          .bind("updatedAt", now)
          .bind("namespaceUuid", namespace.getUuid())
          .bind("name", fragment.getJobName())
          .bind("description", fragment.getDescription())
          .mapTo(UUID.class)
          .one();

      UUID jobContextUuid = handle.createQuery(
          "INSERT INTO job_contexts ("
              + "created_at, context, checksum) "
              + "VALUES (:createdAt, :context, :checksum) "
              + "ON CONFLICT (checksum) "
              + "DO UPDATE SET "
              + "context = :context "
              + "RETURNING uuid")
          .bind("createdAt", now)
          .bind("context", Utils.toJson(fragment.getContext()))
          .bind("checksum", Utils.checksumFor(fragment.getContext()))
          .mapTo(UUID.class)
          .one();

      UUID jobVersionUuid = handle.createQuery("INSERT INTO job_versions ("
          + "created_at, "
          + "updated_at, "
          + "job_uuid, "
          + "job_context_uuid, "
          + "location,"
          + "version"
          + ") VALUES ("
          + ":createdAt, "
          + ":updatedAt, "
          + ":jobUuid, "
          + ":jobContextUuid, "
          + ":location, "
          + ":version) "
          + "ON CONFLICT (version) "
          + "DO UPDATE "
          + " SET updated_at = :updatedAt, job_uuid = :jobUuid, job_context_uuid = :jobContextUuid,"
          + " version = :version"
          + " RETURNING uuid")
          .bind("createdAt", now)
          .bind("updatedAt", now)
          .bind("jobUuid", jobUuid)
          .bind("jobContextUuid", jobContextUuid)
          .bind("location", fragment.getLocation())
          .bind("version", fragment.getJobVersionUuid())
          .mapTo(UUID.class)
          .one();

      handle.createUpdate(
          "UPDATE jobs "
              + "SET current_version_uuid = :currentVersionUuid "
              + "WHERE uuid = :jobUuid")
          .bind("currentVersionUuid", jobVersionUuid)
          .bind("uuid", jobUuid)
          .execute();

      PreparedBatch batch = handle.prepareBatch("INSERT INTO job_versions_io_mapping ("
          + "job_version_uuid, dataset_uuid, io_type) "
          + "VALUES (:jobVersionUuid, :datasetUuid, :ioType) ON CONFLICT DO NOTHING");
      //todo: move to insert/select w/ tuple

      if (fragment.getInputs() != null) {
        for (DatasetFragment dataset : fragment.getInputs()) {
          Optional<Dataset> ds = createDatasetDao().findDataset(dataset.getNamespace(), dataset.getDatasetName());
          if (ds.isPresent()) {
            //Note: This just accumulates inputs and outputs for all runs. It doesn't do a replace.
            batch.bind("jobVersionUuid", jobVersionUuid)
                .bind("datasetUuid", ds.get().getUuid())
                .bind("ioType", IoType.INPUT.name()).add();
          }
        }
      }
      if (fragment.getOutputs() != null) {
        for (DatasetFragment dataset : fragment.getOutputs()) {
          Optional<Dataset> ds = createDatasetDao().findDataset(dataset.getNamespace(), dataset.getDatasetName());
          if (ds.isPresent()) {
            batch.bind("jobVersionUuid", jobVersionUuid)
                .bind("datasetUuid", ds.get().getUuid())
                .bind("ioType", IoType.INPUT.name()).add();
          }
        }
      }
      batch.execute();

      if (fragment.getRunId().isPresent()) {
        handle.createUpdate("INSERT INTO runs_input_mapping (run_uuid, dataset_version_uuid) "
            + "SELECT VALUES :runUuid, dv.uuid FROM dataset_versions dv "
            + "INNER JOIN datasets AS d ON d.uuid = dv.dataset_uuid AND d.current_version_uuid = dv.uuid "
            + "INNER JOIN namespaces n on d.namespace_uuid = n.uuid "
            + "WHERE (d.name, n.name) IN (:datasetVersionUuid)") //todo verify tuple
            .bind("runUuid", fragment.getRunId().get())
            .bindList("datasetVersionUuid", fragment.getInputs())
            .execute();

        handle.createUpdate("UPDATE runs "
            + "SET updated_at = :updatedAt, "
            + "    job_version_uuid = :jobVersionUuid "
            + "WHERE uuid = :rowUuid")
        .bind("updatedAt", now)
        .bind("jobVersionUuid", jobVersionUuid)
        .execute();
      }

      return jobUuid;
    }));

    return this.find(job).get();
  }

  @SqlQuery(
      "SELECT EXISTS (SELECT 1 FROM jobs AS j "
          + "INNER JOIN namespaces AS n "
          + "  ON (n.name = :namespaceName AND "
          + "      j.namespace_uuid = n.uuid AND "
          + "      j.name = :jobName))")
  boolean exists(String namespaceName, String jobName);

  @SqlQuery(
      "SELECT j.*, n.name AS namespace_name FROM jobs AS j "
          + "INNER JOIN namespaces AS n "
          + "  ON (n.name = :namespaceName AND "
          + "      j.namespace_uuid = n.uuid AND "
          + "      j.name = :jobName)")
  Optional<Job> find(String namespaceName, String jobName);

  @SqlQuery(
      "SELECT j.*, n.name AS namespace_name FROM jobs AS j "
          + "INNER JOIN namespaces AS n "
          + "  ON n.name = j.namespace_uuid"
          + "WHERE j.uuid = :uuid")
  Optional<Job> find(UUID uuid);

  @SqlQuery(
      "SELECT j.*, n.name AS namespace_name FROM jobs AS j "
          + "INNER JOIN namespaces AS n "
          + "  ON (n.name = :namespaceName AND j.namespace_uuid = n.uuid) "
          + "ORDER BY j.name "
          + "LIMIT :limit OFFSET :offset")
  List<Job> findAll(String namespaceName, int limit, int offset);

  default Optional<Job> getJobWithDataset(String namespaceName, String jobName) {
    return withHandle(handle -> {
      Optional<Job> job = handle.createQuery("SELECT j.*, n.name AS namespace_name FROM jobs AS j "
          + "INNER JOIN namespaces AS n "
          + "  ON (n.name = :namespaceName AND "
          + "      j.namespace_uuid = n.uuid AND "
          + "      j.name = :jobName)")
          .map(new JobMapper())
          .findOne();

      if (job.isEmpty()) {
        return job;
      }





      return null;
    });
  }


  //job required to reconstitute
//  private Job toJob(@NonNull JobRow jobRow, @Nullable UUID jobVersionUuid) {
//    final UUID currentJobVersionUuid =
//        (jobVersionUuid == null)
//            ? jobRow
//            .getCurrentVersionUuid()
//            .orElseThrow(
//                () ->
//                    new MarquezServiceException(
//                        String.format("Version missing for job row '%s'.", jobRow.getUuid())))
//            : jobVersionUuid;
//    final ExtendedJobVersionRow jobVersionRow =
//        jobVersionDao
//            .findBy(currentJobVersionUuid)
//            .orElseThrow(
//                () ->
//                    new MarquezServiceException(
//                        String.format(
//                            "Version '%s' not found for job row '%s'.",
//                            currentJobVersionUuid, jobRow)));
//    final ImmutableSet<DatasetId> inputs =
//        datasetDao.findAllIn(jobVersionRow.getInputUuids()).stream()
//            .map(Mapper::toDatasetId)
//            .collect(toImmutableSet());
//    final ImmutableSet<DatasetId> outputs =
//        datasetDao.findAllIn(jobVersionRow.getOutputUuids()).stream()
//            .map(Mapper::toDatasetId)
//            .collect(toImmutableSet());
//    final Run runRow =
//        jobVersionRow
//            .getLatestRunUuid()
//            .map(latestRunUuid -> runDao.findBy(latestRunUuid).get())
//            .orElse(null);
//    return Mapper.toJob(
//        jobRow,
//        inputs,
//        outputs,
//        jobVersionRow.getLocation().orElse(null),
//        jobVersionRow.getContext(),
//        runRow);
//  }

//  private ExtendedJobVersionRow createJobVersion(
//      UUID jobId,
//      Version jobVersion,
//      NamespaceName namespaceName,
//      JobName jobName,
//      JobMeta jobMeta) {
//    log.info("Creating version '{}' for job '{}'...", jobVersion.getValue(), jobName.getValue());
//
//    JobContextRow contextRow = getOrCreateJobContextRow(jobMeta.getContext());
//
//    final List<DatasetRow> inputRows = findDatasetRows(jobMeta.getInputs());
//    final List<DatasetRow> outputRows = findDatasetRows(jobMeta.getOutputs());
//    JobVersionRow newJobVersionRow =
//        createJobVersionRow(
//            jobId,
//            contextRow.getUuid(),
//            mapDatasetToUuid(inputRows),
//            mapDatasetToUuid(outputRows),
//            jobMeta.getLocation().orElse(null),
//            jobVersion);
//  private List<UUID> mapDatasetToUuid(List<DatasetRow> datasets) {
//    return datasets.stream().map(DatasetRow::getUuid).collect(Collectors.toList());
//  }
//
//  /**
//   * find the uuids for the datsets based on their namespace/name
//   *
//   * @param datasetIds the namespace/name ids of the datasets
//   * @return their uuids
//   */
//  private List<DatasetRow> findDatasetRows(ImmutableSet<DatasetId> datasetIds) {
//    // group per namespace since that's how we can query the db
//    Map<@NonNull NamespaceName, List<DatasetId>> byNamespace =
//        datasetIds.stream().collect(groupingBy(DatasetId::getNamespace));
//    // query the db for all ds uuids for each namespace and combine them back in one list
//    return byNamespace.entrySet().stream()
//        .flatMap(
//            (e) -> {
//              String namespace = e.getKey().getValue();
//              List<String> names =
//                  e.getValue().stream()
//                      .map(datasetId -> datasetId.getName().getValue())
//                      .collect(toImmutableList());
//              List<DatasetRow> results = datasetDao.findAllIn(namespace, names);
//              if (results.size() < names.size()) {
//                List<String> actual =
//                    results.stream().map(DatasetRow::getName).collect(toImmutableList());
//                throw new MarquezServiceException(
//                    String.format(
//                        "Some datasets not found in namespace %s \nExpected: %s\nActual: %s",
//                        namespace, names, actual));
//              }
//              return results.stream();
//            })
//        .collect(toImmutableList());
//  }

  @SqlQuery("SELECT COUNT(*) FROM jobs")
  int count();
}
