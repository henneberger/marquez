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

package marquez.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.github.jasync.sql.db.pool.ConnectionPool;
import com.github.jasync.sql.db.postgresql.PostgreSQLConnection;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.models.DatasetName;
import marquez.common.models.DatasetType;
import marquez.common.models.DatasetVersionId;
import marquez.common.models.JobName;
import marquez.common.models.JobType;
import marquez.common.models.JobVersionId;
import marquez.common.models.LineageEvent;
import marquez.common.models.LineageEvent.LineageDataset;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.common.models.SourceType;
import marquez.db.DatasetDao;
import marquez.db.DatasetVersionDao;
import marquez.db.JobContextDao;
import marquez.db.JobDao;
import marquez.db.JobVersionDao;
import marquez.db.NamespaceDao;
import marquez.db.RunArgsDao;
import marquez.db.RunDao;
import marquez.db.SourceDao;
import marquez.db.models.DatasetRow;
import marquez.db.models.DatasetVersionRow;
import marquez.db.models.ExtendedDatasetRow;
import marquez.db.models.JobContextRow;
import marquez.db.models.JobRow;
import marquez.db.models.JobVersionRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.RunArgsRow;
import marquez.db.models.RunRow;
import marquez.db.models.SourceRow;
import marquez.service.RunTransitionListener;
import marquez.service.RunTransitionListener.JobInputUpdate;
import marquez.service.RunTransitionListener.RunInput;
import marquez.service.RunTransitionListener.RunTransition;
import marquez.service.models.RunMeta;

@Slf4j
@Path("/api/v1/event")
public class OpenLineageResource {

  private final OpenLineageDao dao;
  private final JobDao jobDao;
  private final RunDao runDao;
  private final JobVersionDao jobVersionDao;
  private final DatasetDao datasetDao;
  private final NamespaceDao namespaceDao;
  private final List<RunTransitionListener> runTransitionListeners;
  private final DatasetVersionDao datasetVersionDao;
  private final SourceDao sourceDao;
  private final RunArgsDao runArgsDao;
  private final JobContextDao jobContextDao;

  private final Executor executor;

  public OpenLineageResource(ConnectionPool<PostgreSQLConnection> source, JobDao jobDao,
      RunDao runDao, JobVersionDao jobVersionDao,
      DatasetDao datasetDao, NamespaceDao namespaceDao,
      List<RunTransitionListener> runTransitionListeners,
      DatasetVersionDao datasetVersionDao, SourceDao sourceDao, RunArgsDao runArgsDao,
      JobContextDao jobContextDao) {
    this.dao = new OpenLineageDao(source);
    this.jobDao = jobDao;
    this.runDao = runDao;
    this.jobVersionDao = jobVersionDao;
    this.datasetDao = datasetDao;
    this.namespaceDao = namespaceDao;
    this.runTransitionListeners = runTransitionListeners;
    this.datasetVersionDao = datasetVersionDao;
    this.sourceDao = sourceDao;
    this.runArgsDao = runArgsDao;
    this.jobContextDao = jobContextDao;

    this.executor = Executors.newSingleThreadExecutor();
  }

//  @Timed
//  @ResponseMetered
//  @ExceptionMetered
  @POST
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  public void create(LineageEvent event, @Suspended final AsyncResponse asyncResponse) {
    dao.write(event, asyncResponse);
    CompletableFuture.supplyAsync(()-> {
      return marquezModel(event);
    }).whenComplete((e, err)->{
      if (err != null) {
        log.error("Open lineage marquez update error", err);
      }
    });
  }

  public static class OpenLineageDao {
    private final ConnectionPool<PostgreSQLConnection> source;

    public OpenLineageDao(ConnectionPool<PostgreSQLConnection> source) {
      this.source = source;
    }

    public void write(LineageEvent event, AsyncResponse asyncResponse) {
      source.sendPreparedStatement(
      "INSERT INTO lineage_event(state_id, name, namespace, time, run_id, state, inputs, outputs) "
          + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)", buildLineageArray(event))
        .whenComplete((result, err) -> {
          if (err != null) {
            log.error("Unexpected error while processing request", err);
            asyncResponse.resume(Response.status(500).build());
          } else {
            asyncResponse.resume(Response.status(201).build());
          }
        });
    }

    private static List<?> buildLineageArray(LineageEvent event) {
      List<Object> list = new ArrayList<>(8);
      list.add(UUID.randomUUID().toString());
      list.add(event.job.name);
      list.add(event.job.namespace);
      list.add(event.transitionTime.toOffsetDateTime());
      list.add(event.run.runId);
      list.add(event.transition);
      list.add(createStrArray(event.inputs));
      list.add(createStrArray(event.outputs));
      return list;
    }

    private static Object createStrArray(List<LineageDataset> inputs) {
      if (inputs == null) return null;
      List<PsqlDataset> in = new ArrayList<>();
      for (LineageDataset d : inputs) {
        in.add(new PsqlDataset(d.name));
      }
      return in;
    }
  }


  private Object marquezModel(LineageEvent event) {
    UUID runUuid = UUID.randomUUID();
    Optional<NamespaceRow> ns = namespaceDao.findBy(event.job.namespace);
    UUID namespaceId;
    if (ns.isEmpty()) {
      namespaceId = UUID.randomUUID();
      namespaceDao.insert(NamespaceRow.builder()
          .name(event.job.namespace)
          .uuid(namespaceId)
        .build());
    } else {
      namespaceId = ns.get().getUuid();
    }

    UUID sourceUuid = UUID.randomUUID();
    String sourceName = UUID.randomUUID().toString();
    Optional<SourceRow> source = sourceDao.findBy(sourceName);
    if (source.isEmpty()) {
      sourceDao.insert(SourceRow.builder()
          .type(SourceType.POSTGRESQL.name())
          .createdAt(event.transitionTime.toInstant())
          .updatedAt(event.transitionTime.toInstant())
          .name(sourceName) //cannot be blank :(
          .connectionUrl("http://"+UUID.randomUUID().toString())
          .uuid(sourceUuid)
          .build());
    } else {
      sourceUuid = source.get().getUuid();
    }
    List<UUID> inputs = new ArrayList<>();
    List<RunInput> runInputs = new ArrayList<>();
    if (event.inputs != null) {
      for (LineageDataset in : event.inputs) {
        UUID inUuid = UUID.randomUUID();
        Optional<ExtendedDatasetRow> ds = datasetDao.find(in.namespace, in.name);
        UUID inVersion = UUID.randomUUID();
        if (ds.isEmpty()) {
          datasetDao.insert(DatasetRow.builder()
              .createdAt(event.transitionTime.toInstant())
              .updatedAt(event.transitionTime.toInstant())
              .name(in.name)
              .namespaceUuid(namespaceId)
              .uuid(inUuid)
              .physicalName(in.name)
              .tagUuids(ImmutableList.of())
              .type(DatasetType.DB_TABLE.name())
              .currentVersionUuid(inVersion)
              .sourceUuid(sourceUuid)
              .build()
          );
        } else {
          inUuid = ds.get().getUuid();
        }
        datasetVersionDao.insert(DatasetVersionRow.builder()
            .uuid(inVersion)
            .fieldUuids(ImmutableList.of())
            .createdAt(event.transitionTime.toInstant())
            .datasetUuid(inUuid)
            .version(UUID.randomUUID())
            .runUuid(runUuid)
            .build());

        inputs.add(inUuid);
        runInputs.add(RunInput.builder()
            .datasetVersionId(DatasetVersionId.builder()
                .name(DatasetName.of(in.name))
                .namespace(NamespaceName.of(event.job.namespace))
                .versionUuid(inVersion)
                .build())
            .build());
      }
    }

    List<UUID> outputs = new ArrayList<>();
    if (event.outputs != null) {
//      for (LineageDataset out : event.outputs) {
//        UUID outUuid = UUID.randomUUID();
//        datasetDao.insert(DatasetRow.builder()
//            .createdAt(event.transitionTime.toInstant())
//            .updatedAt(event.transitionTime.toInstant())
//            .name(out.name)
//            .tagUuids(ImmutableList.of())
//            .sourceUuid(sourceUuid)
//            .physicalName(out.name)
//            .type(DatasetType.STREAM.name())
//            .namespaceUuid(namespaceId)
//            .uuid(outUuid)
//            .build()
//        );
//        outputs.add(outUuid);
//      }
    }

    //WHat is the order here? can I make this now?
    UUID versionUuid = UUID.randomUUID();

    UUID jobUUid = UUID.randomUUID();
    Optional<JobRow> jobRow =
        jobDao.find(event.job.namespace, event.job.name);
    if (jobRow.isEmpty()) {
      jobDao.insert(JobRow.builder()
          .createdAt(event.transitionTime.toInstant())
          .updatedAt(event.transitionTime.toInstant())
          .name(event.job.name)
          .namespaceUuid(namespaceId)
          .namespaceName(event.job.namespace)
          .type(JobType.BATCH.name())
          .uuid(jobUUid)
          .currentVersionUuid(versionUuid)
          .build());
    } else {
      jobUUid = jobRow.get().getUuid();
    }

    UUID jobContextUuid = UUID.randomUUID();
    jobContextDao.insert(JobContextRow.builder()
        .uuid(jobContextUuid)
        .createdAt(event.transitionTime.toInstant())
        .context("{}")
        .checksum(UUID.randomUUID().toString())
        .build());

    jobVersionDao.insert(JobVersionRow.builder()
        .createdAt(event.transitionTime.toInstant())
        .updateAt(event.transitionTime.toInstant())
        .inputUuids(inputs)
        .outputUuids(outputs)
        .uuid(versionUuid)
        .latestRunUuid(runUuid)
        .version(versionUuid)
        .jobContextUuid(jobContextUuid)
        .jobUuid(jobUUid)
        .build());

    UUID runArgsUuid = UUID.randomUUID();
    runArgsDao.insert(RunArgsRow.builder()
        .uuid(runArgsUuid)
        .createdAt(event.transitionTime.toInstant())
        .args("{}")
        .checksum(UUID.randomUUID().toString()) //there is a uniq const here :(
        .build());

    runDao.insert(RunRow.builder()
        .createdAt(event.transitionTime.toInstant())
        .updatedAt(event.transitionTime.toInstant())
        .endedAt(event.transitionTime.toInstant())
        .jobVersionUuid(versionUuid)
        .runArgsUuid(runArgsUuid)
        .inputVersionUuids(inputs)
        .uuid(runUuid)
        .startedAt(event.transitionTime.toInstant())
        .build());

    for (RunTransitionListener l : runTransitionListeners) {
      l.notify(JobInputUpdate.builder()
          .inputs(runInputs)
          .runId(RunId.of(runUuid))
          .runMeta(RunMeta.builder()
              .id(RunId.of(runUuid))
              .nominalEndTime(event.transitionTime.toInstant())
              .nominalStartTime(event.transitionTime.toInstant())
              .build())
          .jobVersionId(JobVersionId.builder()
              .name(JobName.of(event.job.name))
              .namespace(NamespaceName.of(event.job.namespace))
              .versionUuid(versionUuid)
              .build())
      .build());
    }

    return null;
  }
}
