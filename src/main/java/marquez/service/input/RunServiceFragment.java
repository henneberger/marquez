package marquez.service.input;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import marquez.common.models.RunState;

@Builder
@AllArgsConstructor
@Getter
public class RunServiceFragment {
  private final UUID uuid;
  private final Instant createdAt;
  private final Instant updatedAt;
  private final RunArgsFragment runArgs;
  private final Optional<Instant> nominalStartTime;
  private final Optional<Instant> nominalEndTime;

  @NotNull
  private final RunStateFragment runState; //required

  @Builder
  @AllArgsConstructor
  @Getter
  public static class RunStateFragment {
    Instant transitionedAt;
    RunState state;
  }

  @Builder
  @ToString
  @AllArgsConstructor
  @Getter
  public static class RunArgsFragment {
    @NonNull Instant createdAt;
    @NonNull String args;
    @NonNull String checksum;
  }

  @Builder
  @AllArgsConstructor
  @Getter
  public static class JobVersionFragment {
    UUID uuid;
  }
}