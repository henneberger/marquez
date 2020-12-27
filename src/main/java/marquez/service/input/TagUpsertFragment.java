package marquez.service.input;

import java.time.Instant;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@AllArgsConstructor
@Getter
@Builder
public class TagUpsertFragment {
  private final Instant now;
  private final String name;
  private final Optional<String> description;
}