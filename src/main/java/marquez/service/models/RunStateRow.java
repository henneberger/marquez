package marquez.service.models;

import java.time.Instant;
import java.util.UUID;
import lombok.Builder;
import marquez.common.models.RunState;

@Builder
public class RunStateRow {
  public UUID uuid;
  public Instant transitionedAt;
  public UUID runUuid;
  public RunState state;
}
