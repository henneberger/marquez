package marquez.service.models;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@Builder
@Getter
@Setter @ToString
public class Owner {
  private UUID uuid;
  private Instant createdAt;
  private final String name;
  private List<OwnerRecord> record;
}