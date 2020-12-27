package marquez.api;

import static java.time.temporal.ChronoUnit.MILLIS;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import marquez.common.models.RunState;
import marquez.service.models.Run;

public class RunResponse {

  private final Run run;

  public RunResponse(Run run) {
    this.run = run;
  }

  public String getId() {
    return run.getUuid().toString();
  }

  public Optional<Instant> getNominalStartTime() {
    return run.getNominalStartTime();
  }

  public Optional<Instant> getNominalEndTime() {
    return run.getNominalEndTime();
  }

  public Map<String, String> getArgs() {
    if (run.getRunArgs() != null) {
      return run.getRunArgs().getArgs();
    }
    return null;
  }

  public Optional<Instant> getStartedAt() {
    if (run.getStartState() != null) {
      return Optional.ofNullable(run.getStartState().getTransitionedAt());
    }
    return Optional.empty();
  }

  public Optional<Instant> getEndedAt() {
    if (run.getEndState() != null) {
      return Optional.ofNullable(run.getEndState().getTransitionedAt());
    }
    return Optional.empty();
  }

  public Optional<Long> getDurationMs() {
    return this.getEndedAt()
        .flatMap(
            endedAt -> getStartedAt().map(startedAt -> startedAt.until(endedAt, MILLIS)));
  }

  public Instant getCreatedAt() {
    return run.getCreatedAt();
  }

  public Instant getUpdatedAt() {
    return run.getUpdatedAt();
  }

  public RunState getState() {
    return run.getCurrentState().getState();
  }
}
