package marquez.api;

import static marquez.Generator.newIsoTimestamp;
import static marquez.Generator.newTimestamp;
import static marquez.api.JobResource.Jobs;
import static marquez.common.models.ModelGenerator.newJobId;
import static marquez.common.models.ModelGenerator.newRunId;
import static marquez.common.models.RunState.ABORTED;
import static marquez.common.models.RunState.COMPLETED;
import static marquez.common.models.RunState.FAILED;
import static marquez.common.models.RunState.RUNNING;
import static marquez.service.models.ModelGenerator.newJob;
import static marquez.service.models.ModelGenerator.newJobMeta;
import static marquez.service.models.ModelGenerator.newJobWith;
import static marquez.service.models.ModelGenerator.newRun;
import static marquez.service.models.ModelGenerator.newRunMeta;
import static marquez.service.models.ModelGenerator.newRunState;
import static marquez.service.models.ModelGenerator.newRunWith;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.time.Instant;
import java.util.Optional;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import marquez.UnitTests;
import marquez.api.RunsResource.Runs;
import marquez.api.exceptions.JobNotFoundException;
import marquez.api.exceptions.NamespaceNotFoundException;
import marquez.api.exceptions.RunAlreadyExistsException;
import marquez.api.exceptions.RunNotFoundException;
import marquez.common.models.JobId;
import marquez.common.models.JobName;
import marquez.common.models.ModelGenerator;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.db.models.RunArgsRow;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.models.Job;
import marquez.service.models.JobMeta;
import marquez.service.models.Run;
import marquez.service.models.RunMeta;
import marquez.service.models.RunStateRow;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@Category(UnitTests.class)
public class JobResourceTest {
  private static final JobId JOB_ID = newJobId();
  private static final NamespaceName NAMESPACE_NAME = JOB_ID.getNamespace();
  private static final JobName JOB_NAME = JOB_ID.getName();

  private static final Job JOB_0 = newJob();
  private static final Job JOB_1 = newJob();
  private static final Job JOB_2 = newJob();
  private static final ImmutableList<Job> JOBS = ImmutableList.of(JOB_0, JOB_1, JOB_2);

  private static final RunId RUN_ID = newRunId();
  private static final Run RUN_0 = newRun();
  private static final Run RUN_1 = newRun();
  private static final Run RUN_2 = newRun();
  private static final ImmutableList<Run> RUNS = ImmutableList.of(RUN_0, RUN_1, RUN_2);
  private static final String TRANSITIONED_AT = newIsoTimestamp();

  @Rule public MockitoRule rule = MockitoJUnit.rule();

  private JobResource jobResource;
  private RunsResource runsResource;
  private RunResource runResource;
  private MockServiceFactory sf;

  @Before
  public void setUp() {
    this.sf = new MockServiceFactory();
    jobResource = spy(new JobResource(sf));
    runsResource = spy(new RunsResource(sf));
    when(sf.getRunService().exists(RUN_ID)).thenReturn(true);
  }

  @Test
  public void testCreateOrUpdate() throws MarquezServiceException {
    final JobMeta jobMeta = newJobMeta();
    final Job job = toJob(JOB_ID, jobMeta);

    when(sf.getNamespaceService().exists(NAMESPACE_NAME)).thenReturn(true);
    when(sf.getJobService().createOrUpdate(NAMESPACE_NAME, JOB_NAME, jobMeta)).thenReturn(job);

    final Response response = jobResource.createOrUpdate(NAMESPACE_NAME, JOB_NAME, jobMeta);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat((Job) response.getEntity()).isEqualTo(job);
  }

  @Test
  public void testGet() throws MarquezServiceException {
    final Job job = newJobWith(JOB_ID);

    when(sf.getNamespaceService().exists(NAMESPACE_NAME)).thenReturn(true);
    when(sf.getJobService().get(NAMESPACE_NAME, JOB_NAME)).thenReturn(Optional.of(job));

    final Response response = jobResource.get(NAMESPACE_NAME, JOB_NAME);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat((Job) response.getEntity()).isEqualTo(job);
  }

  @Test
  public void testGet_throwOnNamespaceNotFound() throws MarquezServiceException {
    when(sf.getNamespaceService().exists(NAMESPACE_NAME)).thenReturn(false);

    assertThatExceptionOfType(NamespaceNotFoundException.class)
        .isThrownBy(() -> jobResource.get(NAMESPACE_NAME, JOB_NAME))
        .withMessageContaining(String.format("'%s' not found", NAMESPACE_NAME.getValue()));
  }

  @Test
  public void testGet_notFound() throws MarquezServiceException {
    when(sf.getNamespaceService().exists(NAMESPACE_NAME)).thenReturn(true);
    when(sf.getJobService().get(NAMESPACE_NAME, JOB_NAME)).thenReturn(Optional.empty());

    assertThatExceptionOfType(JobNotFoundException.class)
        .isThrownBy(() -> jobResource.get(NAMESPACE_NAME, JOB_NAME))
        .withMessageContaining(String.format("'%s' not found", JOB_NAME.getValue()));
  }

  @Test
  public void testList() throws MarquezServiceException {
    when(sf.getNamespaceService().exists(NAMESPACE_NAME)).thenReturn(true);
    when(sf.getJobService().getAll(NAMESPACE_NAME, 4, 0)).thenReturn(JOBS);

    final Response response = jobResource.list(NAMESPACE_NAME, 4, 0);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(((Jobs) response.getEntity()).getValue()).containsOnly(JOB_0, JOB_1, JOB_2);
  }

  @Test
  public void testList_empty() throws MarquezServiceException {
    when(sf.getNamespaceService().exists(NAMESPACE_NAME)).thenReturn(true);
    when(sf.getJobService().getAll(NAMESPACE_NAME, 4, 0)).thenReturn(ImmutableList.of());

    final Response response = jobResource.list(NAMESPACE_NAME, 4, 0);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(((Jobs) response.getEntity()).getValue()).isEmpty();
  }

  @Test
  public void testCreateRun() throws MarquezServiceException {
    final UriInfo uriInfo = mock(UriInfo.class);

    final RunMeta runMeta = newRunMeta();
    final Run run = toRun(runMeta);
    final URI runLocation = toRunLocation(NAMESPACE_NAME, JOB_NAME, run);

    when(sf.getNamespaceService().exists(NAMESPACE_NAME)).thenReturn(true);
    when(sf.getJobService().exists(NAMESPACE_NAME, JOB_NAME)).thenReturn(true);
    when(sf.getRunService().createRun(NAMESPACE_NAME, JOB_NAME, runMeta)).thenReturn(run);

    doReturn(runLocation).when(runsResource).locationFor(uriInfo, run);

    final Response response = runsResource.create(NAMESPACE_NAME, JOB_NAME, runMeta, uriInfo);
    assertThat(response.getStatus()).isEqualTo(201);
    assertThat(response.getLocation()).isEqualTo(runLocation);
    assertThat((Run) response.getEntity()).isEqualTo(run);
  }

  @Test
  public void testCreateRunWithId() throws MarquezServiceException {
    final UriInfo uriInfo = mock(UriInfo.class);

    final RunId newRunId = newRunId();
    final RunMeta runMeta = newRunMeta(newRunId);
    final Run run = toRun(runMeta);
    final URI runLocation = toRunLocation(NAMESPACE_NAME, JOB_NAME, run);

    when(sf.getNamespaceService().exists(NAMESPACE_NAME)).thenReturn(true);
    when(sf.getJobService().exists(NAMESPACE_NAME, JOB_NAME)).thenReturn(true);
    when(sf.getRunService().createRun(NAMESPACE_NAME, JOB_NAME, runMeta)).thenReturn(run);

    doReturn(runLocation).when(runsResource).locationFor(uriInfo, run);

    final Response response = runsResource.create(NAMESPACE_NAME, JOB_NAME, runMeta, uriInfo);
    assertThat(response.getStatus()).isEqualTo(201);
    assertThat(response.getLocation()).isEqualTo(runLocation);
    assertThat((Run) response.getEntity()).isEqualTo(run);
  }

  @Test
  public void testCreateRun_throwOnIdAlreadyExists() throws MarquezServiceException {
    final UriInfo uriInfo = mock(UriInfo.class);
    final RunId runIdExists = newRunId();
    final RunMeta runMetaWithIdExists = newRunMeta(runIdExists);

    when(sf.getNamespaceService().exists(NAMESPACE_NAME)).thenReturn(true);
    when(sf.getJobService().exists(NAMESPACE_NAME, JOB_NAME)).thenReturn(true);
    when(sf.getRunService().exists(runIdExists)).thenReturn(true);

    assertThatExceptionOfType(RunAlreadyExistsException.class)
        .isThrownBy(
            () -> runsResource.create(NAMESPACE_NAME, JOB_NAME, runMetaWithIdExists, uriInfo))
        .withMessageContaining(String.format("'%s' already exists", runIdExists.getValue()));
  }

  @Test
  public void testCreateRun_throwOnNamespaceNotFound() throws MarquezServiceException {
    final UriInfo uriInfo = mock(UriInfo.class);
    final RunMeta runMeta = newRunMeta();

    when(sf.getNamespaceService().exists(NAMESPACE_NAME)).thenReturn(false);

    assertThatExceptionOfType(NamespaceNotFoundException.class)
        .isThrownBy(() -> runsResource.create(NAMESPACE_NAME, JOB_NAME, runMeta, uriInfo))
        .withMessageContaining(String.format("'%s' not found", NAMESPACE_NAME.getValue()));
  }

  @Test
  public void testCreateRun_throwOnJobNotFound() throws MarquezServiceException {
    final UriInfo uriInfo = mock(UriInfo.class);
    final RunMeta runMeta = newRunMeta();

    when(sf.getNamespaceService().exists(NAMESPACE_NAME)).thenReturn(true);
    when(sf.getJobService().exists(NAMESPACE_NAME, JOB_NAME)).thenReturn(false);

    assertThatExceptionOfType(JobNotFoundException.class)
        .isThrownBy(() -> runsResource.create(NAMESPACE_NAME, JOB_NAME, runMeta, uriInfo))
        .withMessageContaining(String.format("'%s' not found", JOB_NAME.getValue()));
  }

  @Test
  public void testGetRun() throws MarquezServiceException {
    final Run run = newRunWith(RUN_ID);

    when(sf.getRunService().get(RUN_ID)).thenReturn(Optional.of(run));

    final Response response = runsResource.runResourceRoot(RUN_ID).get();
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat((Run) response.getEntity()).isEqualTo(run);
  }

  @Test
  public void testGetRun_notFound() throws MarquezServiceException {
    when(sf.getRunService().get(RUN_ID)).thenReturn(Optional.empty());

    assertThatExceptionOfType(RunNotFoundException.class)
        .isThrownBy(() -> runsResource.runResourceRoot(RUN_ID).get())
        .withMessageContaining(String.format("'%s' not found", RUN_ID.getValue()));
  }

  @Test
  public void testListRuns() throws MarquezServiceException {
    when(sf.getNamespaceService().exists(NAMESPACE_NAME)).thenReturn(true);
    when(sf.getJobService().exists(NAMESPACE_NAME, JOB_NAME)).thenReturn(true);
    when(sf.getRunService().getAllRunsFor(NAMESPACE_NAME, JOB_NAME, 4, 0)).thenReturn(RUNS);

    final Response response = runsResource.list(NAMESPACE_NAME, JOB_NAME, 4, 0);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(((Runs) response.getEntity()).getValue()).containsOnly(RUN_0, RUN_1, RUN_2);
  }

  @Test
  public void testListRuns_empty() throws MarquezServiceException {
    when(sf.getNamespaceService().exists(NAMESPACE_NAME)).thenReturn(true);
    when(sf.getJobService().exists(NAMESPACE_NAME, JOB_NAME)).thenReturn(true);
    when(sf.getRunService().getAllRunsFor(NAMESPACE_NAME, JOB_NAME, 4, 0)).thenReturn(ImmutableList.of());

    final Response response = runsResource.list(NAMESPACE_NAME, JOB_NAME, 4, 0);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(((Runs) response.getEntity()).getValue()).isEmpty();
  }

  @Test
  public void testMarkRunAs_throwOnIdNotFound() throws MarquezServiceException {
    when(sf.getRunService().exists(RUN_ID)).thenReturn(false);

    assertThatExceptionOfType(RunNotFoundException.class)
        .isThrownBy(() -> runsResource.runResourceRoot(RUN_ID).markRunAs(RUNNING, TRANSITIONED_AT))
        .withMessageContaining(String.format("'%s' not found", RUN_ID.getValue()));
  }

  @Test
  public void testMarkRunAsRunning() throws MarquezServiceException {
    when(sf.getRunService().exists(RUN_ID)).thenReturn(true);

    final Run running = newRunWith(RUN_ID, RUNNING, TRANSITIONED_AT);

    when(sf.getRunStateService().markRunAs(eq(RUN_ID), any(), any())).thenReturn(running);
    when(sf.getRunService().get(RUN_ID)).thenReturn(Optional.of(running));

    final Response response = runsResource.runResourceRoot(RUN_ID).markRunAsRunning(TRANSITIONED_AT);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat((Run) response.getEntity()).isEqualTo(running);
  }

  @Test
  public void testMarkRunAsCompleted() throws MarquezServiceException {
    when(sf.getRunService().exists(RUN_ID)).thenReturn(true);
    final Run completed = newRunWith(RUN_ID, COMPLETED, TRANSITIONED_AT);
    when(sf.getRunStateService().markRunAs(eq(RUN_ID), any(), any())).thenReturn(completed);
    when(sf.getRunService().get(RUN_ID)).thenReturn(Optional.of(completed));
    final Response response =
        runsResource.runResourceRoot(RUN_ID).markRunAsCompleted(TRANSITIONED_AT);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat((Run) response.getEntity()).isEqualTo(completed);
  }

  @Test
  public void testMarkRunAsFailed() throws MarquezServiceException {
    when(sf.getRunService().exists(RUN_ID)).thenReturn(true);

    final Run failed = newRunWith(RUN_ID, FAILED, TRANSITIONED_AT);
    when(sf.getRunStateService().markRunAs(eq(RUN_ID), any(), any())).thenReturn(failed);
    when(sf.getRunService().get(RUN_ID)).thenReturn(Optional.of(failed));

    final Response response = runsResource.runResourceRoot(RUN_ID).markRunAsFailed(TRANSITIONED_AT);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat((Run) response.getEntity()).isEqualTo(failed);
  }

  @Test
  public void testMarkRunAsAborted() throws MarquezServiceException {
    when(sf.getRunService().exists(RUN_ID)).thenReturn(true);

    final Run aborted = newRunWith(RUN_ID, ABORTED, TRANSITIONED_AT);
    when(sf.getRunStateService().markRunAs(eq(RUN_ID), any(), any())).thenReturn(aborted);
    when(sf.getRunService().get(RUN_ID)).thenReturn(Optional.of(aborted));

    final Response response = runsResource.runResourceRoot(RUN_ID).markRunAsAborted(TRANSITIONED_AT);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat((Run) response.getEntity()).isEqualTo(aborted);
  }

  static Job toJob(final JobId jobId, final JobMeta jobMeta) {
    final Instant now = newTimestamp();
    return new Job(
        jobId,
        jobMeta.getType(),
        jobId.getName(),
        now,
        now,
        jobMeta.getInputs(),
        jobMeta.getOutputs(),
        jobMeta.getLocation().orElse(null),
        jobMeta.getContext(),
        jobMeta.getDescription().orElse(null),
        null);
  }

  static Run toRun(final RunMeta runMeta) {
    final Instant now = newTimestamp();
    return new Run(
        runMeta.getId().orElseGet(ModelGenerator::newRunId),
        now,
        now,
        null,
        RunArgsRow.builder().args(runMeta.getArgs()).build(),
        runMeta.getNominalStartTime(),
        runMeta.getNominalEndTime(),
        RunStateRow.builder()
            .state(newRunState())
            .build(),
        null,
        null,
        null,null
        );
  }

  static URI toRunLocation(
      final NamespaceName namespaceName, final JobName jobName, final Run run) {
    return URI.create(
        String.format(
            "http://localhost:5000/api/v1/namespaces/%s/jobs/%s/runs/%s",
            namespaceName.getValue(), jobName.getValue(), run.getId().getValue()));
  }
}
