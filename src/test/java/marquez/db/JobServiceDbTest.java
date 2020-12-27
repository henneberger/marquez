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

import marquez.DataAccessTests;
import marquez.IntegrationTests;
import org.junit.experimental.categories.Category;

// TODO: Move test to test/java/marquez/service pkg
@Category({DataAccessTests.class, IntegrationTests.class})
public class JobServiceDbTest {
//
//  @ClassRule public static final JdbiRule dbRule = JdbiRuleInit.init();
//
//  private static final NamespaceName NAMESPACE_NAME = newNamespaceName();
//
//  private static NamespaceDao namespaceDao;
//  private static SourceDao sourceDao;
//  private static DatasetDao datasetDao;
//  private static DatasetVersionDao datasetVersionDao;
//  private static DatasetFieldDao datasetFieldDao;
//  private static TagDao tagDao;
//
//  private static Namespace namespaceRow;
//  private static SourceRow sourceRow;
//  private static List<Tag> tagRows;
//  private static JobService jobService;
//  private static DatasetService datasetService;
//  private static SourceService sourceService;
//
//  public static RunStateService runStateService;
//  private static JobVersionDao jobVersionDao;
//  private static JobDao jobDao;
//  private static JobContextDao contextDao;
//  private static RunDao runDao;
//
//  private static RunTransitionListener listener;
//  private RunService runService;
//
//  @BeforeClass
//  public static void setUpOnce() {
//    final Jdbi jdbi = dbRule.getJdbi();
//    namespaceDao = jdbi.onDemand(NamespaceDao.class);
//    sourceDao = jdbi.onDemand(SourceDao.class);
//    datasetDao = jdbi.onDemand(DatasetDao.class);
//    tagDao = jdbi.onDemand(TagDao.class);
//    datasetVersionDao = jdbi.onDemand(DatasetVersionDao.class);
//    datasetFieldDao = jdbi.onDemand(DatasetFieldDao.class);
//    jobVersionDao = jdbi.onDemand(JobVersionDao.class);
//    jobDao = jdbi.onDemand(JobDao.class);
//    contextDao = jdbi.onDemand(JobContextDao.class);
//    runDao = jdbi.onDemand(RunDao.class);
//
//    namespaceRow = newNamespaceWith(NAMESPACE_NAME);
//    namespaceRow = namespaceDao.upsert(UpsertNamespaceFragment.build(namespaceRow));
//
//    sourceRow = newSource();
//    sourceDao.upsert(sourceRow);
//
//    tagRows = newTagRows(2);
//    tagRows.forEach(tagRow -> tagDao.upsert(TagDao.UpsertTagFragment.build(tagRow)));
//  }
//
//  @Before
//  public void setup() {
//    listener = mock(RunTransitionListener.class);
//    runStateService = new RunStateService(runDao, ImmutableList.of(listener));
//
//    runService =
//        new RunService(
//            jobVersionDao,
//            runDao,
//            runStateService);
//
//    jobService = new JobService(jobDao);
//
//    datasetService =
//        new DatasetService(
//            namespaceDao, sourceDao, datasetDao, datasetFieldDao, datasetVersionDao, tagDao);
//
//    sourceService = new SourceService(sourceDao);
//  }
//
//  @Test
//  /* Tests the condition when an input dataset is added after the run starts (e.g. Bigquery workflow) */
//  public void testLazyInputDataset() {
//    ArgumentCaptor<JobInputUpdate> jobInputUpdateArg =
//        ArgumentCaptor.forClass(JobInputUpdate.class);
//    doNothing().when(listener).notify(jobInputUpdateArg.capture());
//
//    JobName jobName = JobName.of("BIG_QUERY");
//    Job job =
//        jobService.createOrUpdate(
//            NAMESPACE_NAME,
//            jobName,
//            new JobMeta(
//                JobType.BATCH,
//                ImmutableSet.of(),
//                ImmutableSet.of(),
//                Utils.toUrl("https://github.com/repo/test/commit/foo"),
//                ImmutableMap.of(),
//                "description",
//                null));
//    assertThat(job.getId()).isNotNull();
//
//    Run run = runService.createRun(NAMESPACE_NAME, jobName, new RunMeta(null, null, null));
//    assertThat(run.getId()).isNotNull();
//
//    SourceName sn = SourceName.of("bq_source");
//    Source s =
//        sourceService.createOrUpdate(
//            sn, new SourceMeta(SourceType.BIGQUERY, URI.create("http://example.com"), null));
//    assertThat(s.getName()).isNotNull();
//    DatasetName in_dsn = DatasetName.of("INPUT_DATASET");
//    Dataset in_ds =
//        datasetService.createOrUpdate(
//            NAMESPACE_NAME, in_dsn, new DbTableMeta(in_dsn, sn, null, null, null, null));
//
//    DatasetName out_dsn = DatasetName.of("OUTPUT_DATASET");
//    Dataset out_ds =
//        datasetService.createOrUpdate(
//            NAMESPACE_NAME, out_dsn, new DbTableMeta(out_dsn, sn, null, null, null, run.getId()));
//
//    Job update =
//        jobService.createOrUpdate(
//            NAMESPACE_NAME,
//            jobName,
//            new JobMeta(
//                JobType.BATCH,
//                ImmutableSet.of(in_ds.getId()),
//                ImmutableSet.of(out_ds.getId()),
//                Utils.toUrl("https://github.com/repo/test/commit/foo"),
//                ImmutableMap.of(),
//                "description",
//                run.getId()));
//    assertThat(update.getInputs()).hasSize(1);
//    assertThat(update.getOutputs()).hasSize(1);
//
//    Optional<Run> updatedRun = runDao.findBy(run.getId().getValue());
//    assertThat(updatedRun.isPresent()).isEqualTo(true);
//    assertThat(updatedRun.get().getInputs()).hasSize(1);
//
//    List<ExtendedDatasetVersionRow> out_ds_versions =
//        datasetVersionDao.findByRunId(run.getId().getValue());
//    assertThat(out_ds_versions).hasSize(1);
//
//    Optional<Run> run_row = runDao.findBy(run.getId().getValue());
//    assertThat(run_row.isPresent()).isEqualTo(true);
//    assertThat(run_row.get().getInputs()).hasSize(1);
//
//    verify(listener, Mockito.times(2)).notify((JobInputUpdate) any());
//    assertThat(jobInputUpdateArg.getAllValues().get(1).getInputs())
//        .isEqualTo(
//            ImmutableList.of(
//                RunInput.builder()
//                .datasetVersionId(new DatasetVersionId(
//                    NAMESPACE_NAME, in_dsn, run_row.get().getInputs().get(0).getUuid()))
//                .build()));
//  }
//
//  @Test
//  public void testRun() throws MarquezServiceException, MalformedURLException {
//    ArgumentCaptor<RunTransition> runTransitionArg = ArgumentCaptor.forClass(RunTransition.class);
//    doNothing().when(listener).notify(runTransitionArg.capture());
//    ArgumentCaptor<JobInputUpdate> jobInputUpdateArg =
//        ArgumentCaptor.forClass(JobInputUpdate.class);
//    doNothing().when(listener).notify(jobInputUpdateArg.capture());
//    ArgumentCaptor<JobOutputUpdate> jobOutputUpdateArg =
//        ArgumentCaptor.forClass(JobOutputUpdate.class);
//    doNothing().when(listener).notify(jobOutputUpdateArg.capture());
//
//    JobName jobName = JobName.of("MY_JOB");
//    Job job =
//        jobService.createOrUpdate(
//            NAMESPACE_NAME,
//            jobName,
//            new JobMeta(
//                JobType.BATCH,
//                ImmutableSet.of(),
//                ImmutableSet.of(),
//                Utils.toUrl("https://github.com/repo/test/commit/foo"),
//                ImmutableMap.of(),
//                "description",
//                null));
//    assertThat(job.getName()).isEqualTo(jobName);
//    assertThat(job.getId().getNamespace()).isEqualTo(NAMESPACE_NAME);
//    assertThat(job.getId().getName()).isEqualTo(jobName);
//    assertThat(job.getId()).isEqualTo(new JobId(NAMESPACE_NAME, jobName));
//
//    Run run = runService.createRun(NAMESPACE_NAME, jobName, new RunMeta(null, null, null));
//    assertThat(run.getId()).isNotNull();
//    assertThat(run.getStartState() != null).isFalse();
//
//    runStateService.markRunAs(run.getId(), RUNNING, Instant.now());
//    Optional<Run> startedRun = runService.get(run.getId());
//    assertThat(startedRun.isPresent()).isTrue();
//    assertThat(startedRun.get().getStartState()).isNotNull();
//    assertThat(startedRun.get().getEndState()).isNull();
//
//    runStateService.markRunAs(run.getId(), COMPLETED, Instant.now());
//    Optional<Run> endedRun = runService.get(run.getId());
//    assertThat(endedRun.isPresent()).isTrue();
//    assertThat(endedRun.get().getStartState().transitionedAt).isEqualTo(startedRun.get().getStartState().transitionedAt);
//    assertThat(endedRun.get().getEndState()).isNotNull();
//
//    List<Run> allRuns = runService.getAllRunsFor(NAMESPACE_NAME, jobName, 10, 0);
//    assertThat(allRuns.size()).isEqualTo(1);
//    assertThat(allRuns.get(0).getEndState().transitionedAt).isEqualTo(endedRun.get().getEndState().transitionedAt);
//
//    List<JobInputUpdate> jobInputUpdates = jobInputUpdateArg.getAllValues();
//    assertThat(jobInputUpdates.size()).isEqualTo(1);
//    JobInputUpdate jobInputUpdate = jobInputUpdates.get(0);
//    assertThat(jobInputUpdate.getRunId()).isEqualTo(run.getId());
//    assertThat(jobInputUpdate.getJobVersionId().getName()).isEqualTo(jobName);
//
//    List<JobOutputUpdate> jobOutputUpdates = jobOutputUpdateArg.getAllValues();
//    assertThat(jobOutputUpdates.size()).isEqualTo(1);
//    JobOutputUpdate jobOutputUpdate = jobOutputUpdates.get(0);
//    assertThat(jobOutputUpdate.getRunId()).isEqualTo(run.getId());
//
//    List<RunTransition> runTransitions = runTransitionArg.getAllValues();
//    assertThat(runTransitions.size()).isEqualTo(3);
//    RunTransition newRun = runTransitions.get(0);
//    assertThat(newRun.getRunId()).isEqualTo(run.getId());
//    assertThat(newRun.getNewState()).isEqualTo(NEW);
//    RunTransition runningRun = runTransitions.get(1);
//    assertThat(runningRun.getRunId()).isEqualTo(run.getId());
//    assertThat(runningRun.getNewState()).isEqualTo(RUNNING);
//    RunTransition completedRun = runTransitions.get(2);
//    assertThat(completedRun.getRunId()).isEqualTo(run.getId());
//    assertThat(completedRun.getNewState()).isEqualTo(COMPLETED);
//  }
//
//  @Test
//  public void testRunWithId() throws MarquezServiceException {
//    JobName jobName = JobName.of("MY_JOB2");
//    Job job =
//        jobService.createOrUpdate(
//            NAMESPACE_NAME,
//            jobName,
//            new JobMeta(
//                JobType.BATCH,
//                ImmutableSet.of(),
//                ImmutableSet.of(),
//                Utils.toUrl("https://github.com/repo/test/commit/foo"),
//                ImmutableMap.of(),
//                "description",
//                null));
//
//    Run run2 =
//        runService.createRun(NAMESPACE_NAME, job.getName(), new RunMeta(RunId.of(UUID.randomUUID()),
//            null, null, null));
//    Optional<Run> run2Again = runService.get(run2.getId());
//    assertThat(run2Again.isPresent()).isTrue();
//  }
}
