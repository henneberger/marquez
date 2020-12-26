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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import marquez.service.models.Dataset;

@AllArgsConstructor
@EqualsAndHashCode
@ToString
@Builder
public class DatasetVersionRow {
  @Getter @NotNull
  private final UUID uuid;
  @Getter @NotNull private final Instant createdAt;
  @Getter @NotNull private final UUID datasetUuid;
  @Getter @NotNull private final UUID version;
  @Getter @NotNull private final List<UUID> fieldUuids;
  @Nullable private final UUID runUuid;

  @JsonIgnore
  @Getter
  public Dataset dataset;

  public Optional<UUID> getRunUuid() {
    return Optional.ofNullable(runUuid);
  }
}
