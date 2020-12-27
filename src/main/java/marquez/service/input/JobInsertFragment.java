package marquez.service.input;

import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Delegate;

@AllArgsConstructor
@Builder
@Getter
public class JobInsertFragment {
  @Delegate
  private final JobServiceFragment delegate;
  private final UUID jobVersionUuid;
}
