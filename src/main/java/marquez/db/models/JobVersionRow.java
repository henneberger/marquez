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
import lombok.NonNull;
import lombok.ToString;
import marquez.service.models.Job;
import marquez.service.models.Run;

@AllArgsConstructor
@EqualsAndHashCode
@ToString
@Builder
public class JobVersionRow {
  @Getter @NotNull private final UUID uuid;
  @Getter @NotNull private final Instant createdAt;
  @Getter @NotNull
  private final Instant updateAt;
  @Getter @NotNull private final UUID jobUuid;
  @Getter @NotNull private final UUID jobContextUuid;
  @Getter @NotNull private final List<UUID> inputUuids;
  @Getter @NotNull private final List<UUID> outputUuids;
  @Nullable private final String location;
  @Getter @NotNull private final UUID version;
  @Nullable private final UUID latestRunUuid;

  @JsonIgnore
  public JobContextRow jobContext;
  @JsonIgnore
  public Run latestRun;
  @JsonIgnore
  public Job job;

  public boolean hasInputUuids() {
    return !inputUuids.isEmpty();
  }

  public boolean hasOutputUuids() {
    return !outputUuids.isEmpty();
  }

  public Optional<String> getLocation() {
    return Optional.ofNullable(location);
  }

  public Optional<UUID> getLatestRunUuid() {
    return Optional.ofNullable(latestRunUuid);
  }
}
