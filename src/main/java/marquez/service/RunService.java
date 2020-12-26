package marquez.service;

import static com.google.common.base.Preconditions.checkArgument;
import static marquez.common.models.RunState.NEW;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.api.exceptions.RunNotFoundException;
import marquez.common.Utils;
import marquez.common.models.JobName;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.db.JobVersionDao;
import marquez.db.RunDao;
import marquez.db.RunDao.JobVersionFragment;
import marquez.db.RunDao.RunArgsFragment;
import marquez.db.RunDao.RunFragment;
import marquez.db.RunDao.RunStateFragment;
import marquez.db.models.JobVersionRow;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.models.Run;
import marquez.service.models.RunMeta;

@Slf4j
public class RunService implements ServiceMetrics {
  private final JobVersionDao jobVersionDao;
  private final RunDao runDao;
  private final RunStateService runStateService;

  public RunService(
      JobVersionDao jobVersionDao,
      RunDao runDao,
      RunStateService runStateService) {
    this.jobVersionDao = jobVersionDao;
    this.runDao = runDao;
    this.runStateService = runStateService;
  }

  public Run createRun(
      @NonNull NamespaceName namespaceName, @NonNull JobName jobName, @NonNull RunMeta runMeta)
      throws MarquezServiceException {
    log.info("Creating run for job '{}'...", jobName.getValue());

    final JobVersionRow versionRow =
        jobVersionDao.findLatest(namespaceName.getValue(), jobName.getValue()).get();

    Instant now = Instant.now();
    Run run = runDao.create(
        RunFragment.builder()
            .createdAt(now)
            .updatedAt(now)
            .nominalEndTime(runMeta.getNominalEndTime())
            .nominalStartTime(runMeta.getNominalStartTime())
            .runArgs(new RunArgsFragment(now, Utils.toJson(runMeta.getArgs()),
                Utils.checksumFor(runMeta.getArgs())))
            .runState(new RunStateFragment(now, NEW))
            .jobVersion(new JobVersionFragment(versionRow.getUuid()))
            .build());

    runStateService.notify(run, NEW, Optional.empty());
    ServiceMetrics.emitRunStateCounterMetric(NEW);
    log.info(
        "Successfully created run '{}'",
        run.getId().getValue());
    return run;
  }

  public boolean exists(@NonNull RunId runId) throws MarquezServiceException {
    return runDao.exists(runId.getValue());
  }

  public Run get(UUID runId) {
    return runDao.findBy(runId).orElseThrow(() -> new RunNotFoundException(RunId.of(runId)));
  }

  public Optional<Run> get(RunId runId) throws MarquezServiceException {
    return runDao.findBy(runId.getValue());
  }

  public List<Run> getAllRunsFor(
      @NonNull NamespaceName namespaceName, @NonNull JobName jobName, int limit, int offset)
      throws MarquezServiceException {
    checkArgument(limit >= 0, "limit must be >= 0");
    checkArgument(offset >= 0, "offset must be >= 0");

    return runDao.findAll(namespaceName.getValue(), jobName.getValue(), limit, offset);
  }
}
