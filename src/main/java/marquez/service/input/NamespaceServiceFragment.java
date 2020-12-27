package marquez.service.input;

import java.time.Instant;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@AllArgsConstructor
@Getter
@Builder
public class NamespaceServiceFragment {
  private final Instant createdAt;
  private final Instant updatedAt;
  private final String name;
  private final Optional<String> description;
  private final Optional<String> currentOwnerName;
}