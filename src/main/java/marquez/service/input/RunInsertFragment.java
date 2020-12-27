package marquez.service.input;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Delegate;
import marquez.service.input.RunServiceFragment.JobVersionFragment;

@Builder
@AllArgsConstructor
@Getter
public class RunInsertFragment {
  @Delegate
  private final RunServiceFragment delegate;
  private final JobVersionFragment jobVersion;
}