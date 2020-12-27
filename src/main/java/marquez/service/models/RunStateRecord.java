package marquez.service.models;

import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import marquez.common.models.RunState;

@AllArgsConstructor
@Builder
@Getter
@Setter @ToString
public class RunStateRecord {
  private final UUID uuid;
  private Instant transitionedAt;
  private RunState state;
  private Run run;
}
