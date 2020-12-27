package marquez.service;

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import marquez.api.RunMeta;
import marquez.common.models.DatasetName;
import marquez.common.models.DatasetVersionId;
import marquez.common.models.JobName;
import marquez.common.models.JobVersionId;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.common.models.RunState;
import marquez.db.RunDao;
import marquez.service.RunTransitionListener.JobInputUpdate;
import marquez.service.RunTransitionListener.JobOutputUpdate;
import marquez.service.RunTransitionListener.RunInput;
import marquez.service.RunTransitionListener.RunOutput;
import marquez.service.RunTransitionListener.RunTransition;
import marquez.service.input.RunStateInsertFragment;
import marquez.service.models.Run;

@Slf4j
@AllArgsConstructor
public class RunStateService {
  private final RunDao runDao;
  private final List<RunTransitionListener> runTransitionListeners;

  public Run markRunAs(RunStateInsertFragment fragment) {
    log.debug("Marking run with ID '{}' as '{}'...", fragment.getRunId(), fragment.getState());
    Optional<Run> oldRun = runDao.findBy(fragment.getRunId());

    Run run = runDao.createState(fragment, runDao::findByWithDatasets).get();
    notify(run, fragment.getState(), oldRun);
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
    notify(new RunTransition(RunId.of(run.getUuid()),
        oldRun.map(r->r.getCurrentState().getState()).orElse(null),
        run.getCurrentState().getState()
    ));
  }

  private void notifyRunning(Run run) {
    RunMeta runMeta = RunMeta.builder()
            .id(RunId.of(run.getUuid()))
            .args(run.getRunArgs().getArgs())
            .nominalStartTime(run.getNominalStartTime().orElse(null))
            .nominalEndTime(run.getNominalEndTime().orElse(null))
            .build();
    JobVersionId jobVersionId = JobVersionId.builder()
            .versionUuid(run.getJobVersion().getUuid())
            .namespace(NamespaceName.of(run.getJobVersion().getJob().getNamespace().getName()))
            .name(JobName.of(run.getJobVersion().getJob().getName()))
            .build();
    List<RunInput> runInputs = toRunInputs(run);

    notify(new JobInputUpdate(RunId.of(run.getUuid()), runMeta, jobVersionId, runInputs));
  }

  private List<RunInput> toRunInputs(Run run) {
    return run.getInputs().stream()
        .map(d-> new RunInput(DatasetVersionId.builder()
          .versionUuid(d.getUuid())
          .namespace(NamespaceName.of(d.getDataset().getNamespace().getName()))
          .name(DatasetName.of(d.getDataset().getName()))
          .build()))
        .collect(Collectors.toList());
  }

  private void notifyComplete(Run run) {
    JobVersionId jobVersionId = JobVersionId.builder()
        .versionUuid(run.getJobVersion().getUuid())
        .namespace(NamespaceName.of(run.getJobVersion().getJob().getNamespace().getName()))
        .name(JobName.of(run.getJobVersion().getJob().getName()))
        .build();
    List<RunOutput> runOutputs = toRunOutputs(run);
    notify(new JobOutputUpdate(RunId.of(run.getUuid()), jobVersionId, runOutputs));
  }

  private List<RunOutput> toRunOutputs(Run run) {
    return run.getOutputs().stream()
        .map(d -> new RunOutput(
            DatasetVersionId.builder()
                .versionUuid(d.getUuid())
                .namespace(NamespaceName.of(d.getDataset().getNamespace().getName()))
                .name(DatasetName.of(d.getDataset().getName()))
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
