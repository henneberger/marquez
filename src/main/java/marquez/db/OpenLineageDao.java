package marquez.db;

import com.github.jasync.sql.db.pool.ConnectionPool;
import com.github.jasync.sql.db.postgresql.PostgreSQLConnection;
import java.util.List;
import javax.ws.rs.container.AsyncResponse;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.models.LineageEvent;
import marquez.service.RunTransitionListener;
import marquez.service.ServiceFactory;

@Slf4j
public class OpenLineageDao {

  private final ConnectionPool<PostgreSQLConnection> source;
  private final ServiceFactory sf;
  private final List<RunTransitionListener> runTransitionListeners;

  public OpenLineageDao(
      @NonNull ConnectionPool<PostgreSQLConnection> source,
      @NonNull ServiceFactory serviceFactory,
      @NonNull List<RunTransitionListener> runTransitionListeners) {
    this.source = source;
    this.sf = serviceFactory;
    this.runTransitionListeners = runTransitionListeners;
  }

  public void write(LineageEvent event, AsyncResponse asyncResponse) {
//    source.sendPreparedStatement(
//        "INSERT INTO lineage_event(state_id, name, namespace, time, run_id, state, inputs, outputs) "
//            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)", buildLineageArray(event))
//        .whenComplete((result, err) -> {
//          if (err != null) {
//            log.error("Unexpected error while processing request", err);
//            asyncResponse.resume(Response.status(500).build());
//          } else {
//            asyncResponse.resume(Response.status(201).build());
//          }
//        });
  }
//
//  private static List<?> buildLineageArray(LineageEvent event) {
//    List<Object> list = new ArrayList<>(8);
//    list.add(UUID.randomUUID().toString());
//    list.add(event.job.name);
//    list.add(event.job.namespace);
//    list.add(event.transitionTime.toOffsetDateTime());
//    list.add(event.run.runId);
//    list.add(event.transition);
//    list.add(createStrArray(event.inputs));
//    list.add(createStrArray(event.outputs));
//    return list;
//  }
//
//  private static Object createStrArray(List<LineageDataset> inputs) {
//    if (inputs == null) {
//      return null;
//    }
//    List<PsqlDataset> in = new ArrayList<>();
//    for (LineageDataset d : inputs) {
//      in.add(new PsqlDataset(d.name));
//    }
//    return in;
//  }
//
//  public Object marquezModel(LineageEvent event) {
//    UUID runUuid = UUID.randomUUID();
//    Namespace namespace = sf.getNamespaceDao().upsert(UpsertNamespaceFragment.build(event.job.namespace));
//
//    String sourceName = UUID.randomUUID().toString();
//    Source source = sf.getSourceDao().upsert(SourceRow.builder()
//        .type(SourceType.POSTGRESQL.name())
//        .createdAt(event.transitionTime.toInstant())
//        .updatedAt(event.transitionTime.toInstant())
//        .name(sourceName)
//        .connectionUrl("http://"+UUID.randomUUID().toString())
//        .build());
//
//    List<UUID> inputs = new ArrayList<>();
//    List<RunInput> runInputs = new ArrayList<>();
//    if (event.inputs != null) {
//      for (LineageDataset in : event.inputs) {
//        UUID inUuid = UUID.randomUUID();
//        Optional<ExtendedDatasetRow> ds = sf.getDatasetDao().find(in.namespace, in.name);
//        UUID inVersion = UUID.randomUUID();
//        if (ds.isEmpty()) {
//          sf.getDatasetDao().insert(DatasetRow.builder()
//              .createdAt(event.transitionTime.toInstant())
//              .updatedAt(event.transitionTime.toInstant())
//              .name(in.name)
//              .namespaceUuid(namespace.getUuid())
//              .uuid(inUuid)
//              .physicalName(in.name)
//              .tagUuids(ImmutableList.of())
//              .type(DatasetType.DB_TABLE.name())
//              .currentVersionUuid(inVersion)
//              .sourceUuid(source.getUuid())
//              .build()
//          );
//        } else {
//          inUuid = ds.get().getUuid();
//        }
//        sf.getDatasetVersionDao().insert(DatasetVersionRow.builder()
//            .uuid(inVersion)
//            .fieldUuids(ImmutableList.of())
//            .createdAt(event.transitionTime.toInstant())
//            .datasetUuid(inUuid)
//            .version(UUID.randomUUID())
//            .runUuid(runUuid)
//            .build());
//
//        inputs.add(inUuid);
//        runInputs.add(RunInput.builder()
//            .datasetVersionId(DatasetVersionId.builder()
//                .name(DatasetName.of(in.name))
//                .namespace(NamespaceName.of(event.job.namespace))
//                .versionUuid(inVersion)
//                .build())
//            .build());
//      }
//    }
//
//    List<UUID> outputs = new ArrayList<>();
//    if (event.outputs != null) {
////      for (LineageDataset out : event.outputs) {
////        UUID outUuid = UUID.randomUUID();
////        datasetDao.insert(DatasetRow.builder()
////            .createdAt(event.transitionTime.toInstant())
////            .updatedAt(event.transitionTime.toInstant())
////            .name(out.name)
////            .tagUuids(ImmutableList.of())
////            .sourceUuid(sourceUuid)
////            .physicalName(out.name)
////            .type(DatasetType.STREAM.name())
////            .namespaceUuid(namespaceId)
////            .uuid(outUuid)
////            .build()
////        );
////        outputs.add(outUuid);
////      }
//    }
//
//    //WHat is the order here? can I make this now?
//
//    UUID jobUUid = UUID.randomUUID();
////    Optional<JobRow> jobRow =
////        sf.getJobDao().find(event.job.namespace, event.job.name);
////    if (jobRow.isEmpty()) {
////      sf.getJobDao().insert(JobRow.builder()
////          .createdAt(event.transitionTime.toInstant())
////          .updatedAt(event.transitionTime.toInstant())
////          .name(event.job.name)
////          .namespaceUuid(namespace.getUuid())
////          .namespaceName(event.job.namespace)
////          .type(JobType.BATCH.name())
////          .uuid(jobUUid)
//////          .currentVersionUuid(versionUuid)
////          .build());
////    } else {
////      jobUUid = jobRow.get().getUuid();
////    }
//
//    UUID jobContextUuid = UUID.randomUUID();
//    sf.getJobContextDao().insert(JobContextRow.builder()
//        .uuid(jobContextUuid)
//        .createdAt(event.transitionTime.toInstant())
//        .context("{}")
//        .checksum(UUID.randomUUID().toString())
//        .build());
//
//    UUID versionUuid = UUID.randomUUID();
//    sf.getJobVersionDao().insert(JobVersion.builder()
//        .createdAt(event.transitionTime.toInstant())
//        .updateAt(event.transitionTime.toInstant())
//        .inputUuids(inputs)
//        .outputUuids(outputs)
//        .uuid(versionUuid)
//        .latestRunUuid(runUuid)
//        .version(versionUuid) //version function
//        .jobContextUuid(jobContextUuid)
//        .jobUuid(jobUUid)
//        .build());
//
////    jobDao.updateVersion(jobUUid, event.transitionTime.toInstant(), versionUuid);
////
////    UUID runArgsUuid = UUID.randomUUID();
////    runArgsDao.insert(RunArgsRow.builder()
////        .uuid(runArgsUuid)
////        .createdAt(event.transitionTime.toInstant())
////        .args("{}")
////        .checksum(UUID.randomUUID().toString()) //there is a uniq const here :(
////        .build());
////
////    sf.getRunDao().insert(RunRow.builder()
////        .createdAt(event.transitionTime.toInstant())
////        .updatedAt(event.transitionTime.toInstant())
////        .endedAt(event.transitionTime.toInstant())
////        .jobVersionUuid(versionUuid)
//////        .runArgsUuid(runArgsUuid)
////        .inputVersionUuids(inputs)
////        .uuid(runUuid)
////        .startedAt(event.transitionTime.toInstant())
////        .build());
//
//    for (RunTransitionListener l : runTransitionListeners) {
//      l.notify(JobInputUpdate.builder()
//          .inputs(runInputs)
//          .runId(RunId.of(runUuid))
//          .runMeta(RunMeta.builder()
//              .id(RunId.of(runUuid))
//              .nominalEndTime(event.transitionTime.toInstant())
//              .nominalStartTime(event.transitionTime.toInstant())
//              .build())
//          .jobVersionId(JobVersionId.builder()
//              .name(JobName.of(event.job.name))
//              .namespace(NamespaceName.of(event.job.namespace))
//              .versionUuid(versionUuid)
//              .build())
//          .build());
//    }
//
//    return null;
//  }
}
