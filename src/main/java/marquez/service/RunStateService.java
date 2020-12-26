package marquez.service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.models.DatasetVersionId;
import marquez.common.models.JobVersionId;
import marquez.common.models.RunId;
import marquez.common.models.RunState;
import marquez.db.JobVersionDao;
import marquez.db.RunDao;
import marquez.service.RunTransitionListener.JobInputUpdate;
import marquez.service.RunTransitionListener.JobOutputUpdate;
import marquez.service.RunTransitionListener.RunInput;
import marquez.service.RunTransitionListener.RunOutput;
import marquez.service.RunTransitionListener.RunTransition;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.models.Run;
import marquez.service.models.RunMeta;

@Slf4j
@AllArgsConstructor
public class RunStateService {
  private final RunDao runDao;
  private final List<RunTransitionListener> runTransitionListeners;

  public Run markRunAs(@NonNull RunId runId, @NonNull RunState state, @NonNull Instant transitionedAt /*moved to not null*/)throws MarquezServiceException {
    log.debug("Marking run with ID '{}' as '{}'...", runId, state);
    Optional<Run> oldRun = runDao.findBy(runId.getValue());

    Run run = runDao.createState(RunDao.RunStateCreateFragment.builder()
        .runId(runId.getValue())
        .state(state)
        .transitionedAt(transitionedAt)
        .build(), runDao::findByWithDatasets)
        .get();
    notify(run, state, oldRun);
    return run;
  }

  public void notify(Run run, RunState state, Optional<Run> oldRun) {
    switch (state) {
      case NEW:
        break;
      case RUNNING:
        notifyRunning(run);
        break;
      case COMPLETED:
      case ABORTED:
      case FAILED:
        notifyComplete(run);
        break;
    }
    notify(new RunTransition(run.getId(),
        oldRun.map(Run::getState).orElse(null),
        run.getCurrentState().state
    ));
  }

  private void notifyRunning(Run run) {
    notify(new JobInputUpdate(run.getId(),
        RunMeta.builder()
          .id(run.getId())
          .args(run.getRunArgs().getArgs())
          .nominalStartTime(run.getNominalStartTime().orElse(null))
          .nominalEndTime(run.getNominalEndTime().orElse(null))
        .build(), JobVersionId.builder()
            .versionUuid(run.getJobVersion().getUuid())
            .namespace(run.jobVersion.job.getNamespace())
            .name(run.jobVersion.job.getName())
        .build(), toRunInputs(run)));
  }

  private List<RunInput> toRunInputs(Run run) {
    return run.inputs.stream()
        .map(d-> new RunInput(DatasetVersionId.builder()
          .versionUuid(d.getUuid())
          .namespace(d.dataset.getNamespace())
          .name(d.dataset.getName())
          .build()))
        .collect(Collectors.toList());
  }

  private void notifyComplete(Run run) {
    notify(new JobOutputUpdate(run.getId(), JobVersionId.builder()
        .versionUuid(run.getJobVersion().getUuid())
        .namespace(run.jobVersion.job.namespace_.getName())
        .name(run.jobVersion.job.getName())
        .build(), toRunOutputs(run)));
  }

  private List<RunOutput> toRunOutputs(Run run) {
    return run.getOutputs().stream()
        .map(d -> new RunOutput(
            DatasetVersionId.builder()
                .versionUuid(d.getUuid())
                .namespace(d.dataset.getNamespace())
                .name(d.dataset.getName())
                .build()))
        .collect(Collectors.toList());
  }

  private void notify(JobInputUpdate update) {
    notify(RunTransitionListener::notify, update);
  }

  private void notify(JobOutputUpdate update) {
    notify(RunTransitionListener::notify, update);
  }

  private void notify(RunTransition transition) {
    notify(RunTransitionListener::notify, transition);
  }

  private <T> void notify(BiConsumer<RunTransitionListener, T> f, T param) {
    for (RunTransitionListener runTransitionListener : runTransitionListeners) {
      try {
        f.accept(runTransitionListener, param);
      } catch (Exception e) {
        log.error("Exception from listener " + runTransitionListener, e);
      }
    }
  }
}
