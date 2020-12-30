package marquez.spark.agent.lifecycle;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import marquez.spark.agent.LineageEvent;
import marquez.spark.agent.LineageEvent.LineageDataset;
import marquez.spark.agent.LineageEvent.LineageJob;
import marquez.spark.agent.LineageEvent.LineageRun;
import marquez.spark.agent.LineageEvent.RunFacet;
import marquez.spark.agent.MarquezContext;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.FileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.sql.sources.BaseRelation;
import org.codehaus.jackson.map.ObjectMapper;
import scala.collection.JavaConversions;

@Slf4j
public class SparkSQLExecutionContext implements ExecutionContext {
  private final long executionId;

  private MarquezContext marquezContext;

  private String namespace = Optional.ofNullable(System.getenv("MARQUEZ_NAMESPACE")).orElse("default");
  private boolean success;
  private Exception error;

  public SparkSQLExecutionContext(long executionId, MarquezContext marquezContext) {
    this.executionId = executionId;
    this.marquezContext = marquezContext;
  }

  public void start(SparkListenerSQLExecutionStart startEvent) {
//    log.info("Spark sql execution started " + startEvent);
  }

  private List<LineageDataset> buildInputs(LogicalPlan logical) {
    List<LineageDataset> inputDatasets = new ArrayList<>();
    Collection<LogicalPlan> leaves = JavaConversions.asJavaCollection(logical.collectLeaves());
    for (LogicalPlan leaf : leaves) {
      if (!(leaf instanceof LogicalRelation)) {
        continue;
      }
      LogicalRelation lr = (LogicalRelation) leaf;
      BaseRelation lrr = lr.relation();
      if (!(lrr instanceof HadoopFsRelation)) {
        continue;
      }
      FileIndex location = ((HadoopFsRelation)lrr).location();
      Collection<Path> rootPaths = JavaConversions.asJavaCollection(location.rootPaths());
      for (Path rootPath : rootPaths) {
        LineageDataset.LineageDatasetBuilder lineageDataset = LineageDataset.builder()
            .namespace(namespace)
            .name(rootPath.toUri().toString().replaceAll(":", "_"));
        inputDatasets.add(lineageDataset.build());
      }
    }

    return inputDatasets;
  }

  private List<LineageDataset> buildOutputs(LogicalPlan logical) {
    List<LineageDataset> inputDatasets = new ArrayList<>();
    if (logical instanceof InsertIntoHadoopFsRelationCommand) {
      InsertIntoHadoopFsRelationCommand insert = (InsertIntoHadoopFsRelationCommand)logical;
      inputDatasets.add(LineageDataset.builder()
          .namespace(namespace)
          .name(insert.outputPath().toUri().toString())
          .build());
    }
    return inputDatasets;
  }

  private LineageRun buildRun(Object logicalPlanFacet, Object physicalPlanFacet) {
    return LineageRun.builder()
        .runId(UUID.randomUUID().toString())
        .facets(
          RunFacet.builder()
          .additional(ImmutableMap.of(
              LogicalPlan.class.getName(), logicalPlanFacet,
              SparkPlan.class.getName(), physicalPlanFacet)
          ).build())
        .build();
  }

  private Object buildLogicalPlanFacet(LogicalPlan plan) {
    try {
      return new ObjectMapper().readTree(plan.toJSON());
    } catch (IOException e) {
      log.info("Unable to read logical plan", e);
      return null;
    }
  }

  private Object buildPhysicalPlanFacet(SparkPlan executedPlan) {
    try {
      return new ObjectMapper().readTree(executedPlan.toJSON());
    } catch (IOException e) {
      log.info("Unable to read physical plan", e);
      return null;
    }
  }

  private LineageJob buildJob() {
    return LineageJob.builder()
        .namespace(namespace)
        .name("SparkJob")
        .build();
  }

  public void end(SparkListenerSQLExecutionEnd endEvent) {
    QueryExecution queryExecution = SQLExecution.getQueryExecution(executionId);
    if (queryExecution == null) {
      log.info("No execution info {}", queryExecution);
      return;
    }
    LineageEvent event = LineageEvent.builder()
        .inputs(buildInputs(queryExecution.logical()))
        .outputs(buildOutputs(queryExecution.logical()))
        .run(buildRun(
            buildLogicalPlanFacet(queryExecution.logical()),
            buildPhysicalPlanFacet(queryExecution.executedPlan())))
        .job(buildJob())
        .eventTime(ZonedDateTime.now())
        .eventType("COMPLETE")
        .build();

    marquezContext.emit(event);
  }
//
  @Override
  public void setActiveJob(ActiveJob activeJob) {
//    jobId = activeJob.jobId();
//    RDD<?> finalRdd = activeJob.finalStage().rdd();
//    logger.info("Registered Active job as part of spark-sql:" + activeJob.jobId() + " rdd: " + finalRdd);
//    Set<RDD<?>> rdds = Rdds.flattenRDDs(finalRdd);
//    for (RDD<?> rdd : rdds) {
//      Configuration config = SparkListener.removeConfigForRDD(rdd);
//      if (config != null) {
//        extraInfo.append("unexpected rdd => conf mapping\n  ").append(rdd).append(" => ").append(config).append("\n");
//        logger.warn("This should not be here: " + rdd + " => " + config);
//      }
//    }
  }

  @Override
  public void start(SparkListenerJobStart jobStart) {
//    this.startTime = jobStart.time();
//    log.info("Starting job as part of spark-sql:" + jobStart.jobId());
//    extraInfo.append(Rdds.toString(jobStart));
  }
//
  @Override
  public void end(SparkListenerJobEnd jobEnd) {
    log.info("Ending job as part of spark-sql:" + jobEnd.jobId());
    if (jobEnd.jobResult() instanceof JobFailed) {
      success = false;
      error = ((JobFailed)jobEnd.jobResult()).exception();
    } else if (jobEnd.jobResult().getClass().getSimpleName().startsWith("JobSucceeded")){
      success = true;
    } else {
//      extraInfo.append("Unknown status: " ).append(jobEnd.jobResult()).append("\n");
    }

  }
}

