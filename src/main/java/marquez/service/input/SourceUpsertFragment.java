package marquez.service.input;

import java.time.Instant;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import marquez.common.models.SourceType;

@Builder
@Getter
@AllArgsConstructor
public class SourceUpsertFragment {
  private final SourceType type;
  private final String name;

  private final Instant createdAt;
  private final Instant updatedAt;
  private final String connectionUrl;
  private final Optional<String> description;
}