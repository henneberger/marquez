package marquez.service.input;

import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import marquez.common.models.RunState;

@AllArgsConstructor
@Builder
@Getter
public class RunStateInsertFragment {
  private final UUID runId;
  private final RunState state;
  private final Instant transitionedAt;
}