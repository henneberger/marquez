/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package marquez.db.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import lombok.Value;
import marquez.service.models.Owner;

@Value
public class NamespaceOwnershipRow {
  @NotNull UUID uuid;
  @NotNull Instant startedAt;
  @Nullable Instant endedAt;
  @NotNull UUID namespaceUuid;
  @NotNull UUID ownerUuid;

  @JsonIgnore
  public List<Owner> owners;

  public Optional<Instant> getEndedAt() {
    return Optional.ofNullable(endedAt);
  }

  public boolean hasOwnershipEnded() {
    return (endedAt == null) ? false : endedAt.isAfter(startedAt);
  }
}
