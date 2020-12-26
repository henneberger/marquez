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

package marquez.service.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import marquez.common.models.NamespaceName;
import marquez.common.models.OwnerName;
import marquez.db.models.NamespaceOwnershipRow;

@Getter
@AllArgsConstructor
public class Namespace {
  @JsonIgnore @Setter UUID uuid;
  @NonNull NamespaceName name;
  @NonNull Instant createdAt;
  @NonNull Instant updatedAt;
  @Setter @NonNull OwnerName ownerName;
  @Nullable String description;

  @JsonIgnore
  public List<NamespaceOwnershipRow> owners;

  public Namespace(UUID uuid, NamespaceName name, Instant createdAt, Instant updatedAt,
      String description) {
    this.uuid = uuid;
    this.name = name;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
    this.description = description;
  }

  public Optional<String> getDescription() {
    return Optional.ofNullable(description);
  }
}
