package marquez.service;

import static com.google.common.base.Preconditions.checkArgument;
import static marquez.common.models.RunState.NEW;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.db.JobVersionDao;
import marquez.db.RunDao;
import marquez.service.input.RunInsertFragment;
import marquez.service.input.RunServiceFragment;
import marquez.service.input.RunServiceFragment.JobVersionFragment;
import marquez.service.models.JobVersion;
import marquez.service.models.Run;

@Slf4j
@AllArgsConstructor
public class RunService implements ServiceMetrics {
  private final JobVersionDao jobVersionDao;
  private final RunDao runDao;
  private final RunStateService runStateService;

  public Run createRun(String namespace, String jobName, RunServiceFragment fragment) {
    log.info("Creating run for job '{}'...", jobName);

    final JobVersion versionRow =
        jobVersionDao.findLatest(namespace, jobName).get();
    RunInsertFragment insertFragment = RunInsertFragment.builder()
        .delegate(fragment)
        .jobVersion(new JobVersionFragment(versionRow.getUuid()))
        .build();

    Run run = runDao.create(insertFragment);

    runStateService.notify(run, NEW, Optional.empty());
    ServiceMetrics.emitRunStateCounterMetric(NEW);
    log.info(
        "Successfully created run '{}'",
        run.getUuid());
    return run;
  }

  public boolean exists(@NonNull UUID runId) {
    return runDao.exists(runId);
  }

  public Optional<Run> get(UUID runId) {
    return runDao.findBy(runId);
  }

  public List<Run> getAllRunsFor(@NonNull String namespace, @NonNull String jobName, int limit, int offset) {
    checkArgument(limit >= 0, "limit must be >= 0");
    checkArgument(offset >= 0, "offset must be >= 0");

    return runDao.findAll(namespace, jobName, limit, offset);
  }
}
