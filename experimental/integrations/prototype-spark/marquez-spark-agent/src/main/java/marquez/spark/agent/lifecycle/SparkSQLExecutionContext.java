package marquez.spark.agent.lifecycle;

import static scala.collection.JavaConversions.asJavaCollection;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import marquez.client.models.LineageEvent;
import marquez.client.models.LineageEvent.Dataset;
import marquez.client.models.LineageEvent.RunFacet;
import marquez.spark.agent.MarquezContext;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.JobResult;
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

@Slf4j
public class SparkSQLExecutionContext implements ExecutionContext {
  private final long executionId;

  private MarquezContext marquezContext;

  public SparkSQLExecutionContext(long executionId, MarquezContext marquezContext) {
    this.executionId = executionId;
    this.marquezContext = marquezContext;
  }

  public void start(SparkListenerSQLExecutionStart startEvent) {
  }

  public void end(SparkListenerSQLExecutionEnd endEvent) {
  }

  @Override
  public void setActiveJob(ActiveJob activeJob) {
  }

  @Override
  public void start(SparkListenerJobStart jobStart) {
    log.info("Starting job as part of spark-sql:" + jobStart.jobId());
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
        .eventType("START")
        .producer("spark")
        .build();

    marquezContext.emit(event);
  }

  @Override
  public void end(SparkListenerJobEnd jobEnd) {
    log.info("Ending job as part of spark-sql:" + jobEnd.jobId());
    QueryExecution queryExecution = SQLExecution.getQueryExecution(executionId);
    if (queryExecution == null) {
      log.info("No execution info {}", queryExecution);
      return;
    }
    if (jobEnd.jobResult() instanceof JobFailed) {
//      error = ((JobFailed)jobEnd.jobResult()).exception();
    }

    LineageEvent event = LineageEvent.builder()
        .inputs(buildInputs(queryExecution.logical()))
        .outputs(buildOutputs(queryExecution.logical()))
        .run(buildRun(
            buildLogicalPlanFacet(queryExecution.logical()),
            buildPhysicalPlanFacet(queryExecution.executedPlan())))
        .job(buildJob())
        .eventTime(ZonedDateTime.now())
        .eventType(getEventType(jobEnd.jobResult()))
        .producer("spark")
        .build();

    marquezContext.emit(event);
  }

  protected String getEventType(JobResult jobResult) {
    if (jobResult.getClass().getSimpleName().startsWith("JobSucceeded")){
      return "COMPLETE";
    }
    return "FAIL";
  }

  private List<Dataset> buildInputs(LogicalPlan logical) {
    List<Dataset> inputDatasets = new ArrayList<>();
    Collection<LogicalPlan> leaves = asJavaCollection(logical.collectLeaves());
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
      Collection<Path> rootPaths = asJavaCollection(location.rootPaths());
      for (Path rootPath : rootPaths) {
        Dataset.DatasetBuilder lineageDataset = Dataset.builder()
            .namespace(marquezContext.getJobNamespace())
            .name(rootPath.toUri().toString().replaceAll(":", "_"));
        inputDatasets.add(lineageDataset.build());
      }
    }

    return inputDatasets;
  }

  private List<Dataset> buildOutputs(LogicalPlan logical) {
    List<Dataset> inputDatasets = new ArrayList<>();
    if (logical instanceof InsertIntoHadoopFsRelationCommand) {
      InsertIntoHadoopFsRelationCommand insert = (InsertIntoHadoopFsRelationCommand)logical;
      inputDatasets.add(Dataset.builder()
          .namespace(marquezContext.getJobNamespace())
          .name(insert.outputPath().toUri().toString())
          .build());
    }
    return inputDatasets;
  }

  private LineageEvent.Run buildRun(Object logicalPlanFacet, Object physicalPlanFacet) {
    return LineageEvent.Run.builder()
        .runId(marquezContext.getParentRunId())
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
      return marquezContext.getMapper().readTree(plan.toJSON());
    } catch (IOException e) {
      log.info("Unable to read logical plan", e);
      return null;
    }
  }

  private Object buildPhysicalPlanFacet(SparkPlan executedPlan) {
    try {
      return marquezContext.getMapper().readTree(executedPlan.toJSON());
    } catch (IOException e) {
      log.info("Unable to read physical plan", e);
      return null;
    }
  }

  private LineageEvent.Job buildJob() {
    return LineageEvent.Job.builder()
        .namespace(marquezContext.getJobNamespace())
        .name(marquezContext.getJobName())
        .build();
  }
}