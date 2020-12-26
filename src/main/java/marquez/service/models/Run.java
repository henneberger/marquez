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

import static java.time.temporal.ChronoUnit.MILLIS;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import marquez.common.models.RunId;
import marquez.common.models.RunState;
import marquez.db.models.DatasetVersionRow;
import marquez.db.models.ExtendedDatasetVersionRow;
import marquez.db.models.JobVersionRow;
import marquez.db.models.RunArgsRow;

@EqualsAndHashCode
@ToString
@AllArgsConstructor
public final class Run {
  @Getter private final RunId id;
  @Getter private final Instant createdAt;
  @Getter private final Instant updatedAt;
  @JsonIgnore
  @Getter
  public JobVersionRow jobVersion;
  @JsonIgnore
  @Getter
  public RunArgsRow runArgs;

  @Nullable private final Optional<Instant> nominalStartTime;
  @Nullable private final Optional<Instant> nominalEndTime;

  @JsonIgnore
  @Getter
  public RunStateRow currentState;

  @JsonIgnore
  @Getter
  public RunStateRow startState;

  @JsonIgnore
  @Getter
  public RunStateRow endState;

  @JsonIgnore
  @Getter
  public List<DatasetVersionRow> inputs;

  @JsonIgnore
  @Getter
  public List<ExtendedDatasetVersionRow> outputs;


  public RunState getState() {
    if (currentState != null) {
      return currentState.state;
    }
    return null;
  }
  public Optional<Instant> getNominalStartTime() {
    return nominalStartTime;
  }

  public Optional<Instant> getNominalEndTime() {
    return nominalEndTime;
  }

  public Optional<Instant> getStartedAt() {
    if (startState != null) {
      return Optional.ofNullable(startState.transitionedAt);
    }
    return Optional.empty();
  }

  public Optional<Instant> getEndedAt() {
    if (endState != null) {
      return Optional.ofNullable(endState.transitionedAt);
    }
    return Optional.empty();
  }

  public Optional<Long> getDurationMs() {
    return this.getEndedAt()
        .flatMap(
            endedAt -> getStartedAt().map(startedAt -> startedAt.until(endedAt, MILLIS)));
  }
}
