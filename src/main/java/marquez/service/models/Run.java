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

import java.time.Instant;
import java.util.List;
import java.util.Optional;
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
public final class Run {
  private final UUID uuid;
  private Instant createdAt;
  private Instant updatedAt;
  private Optional<Instant> nominalStartTime;
  private Optional<Instant> nominalEndTime;

  private JobVersion jobVersion;
  private RunArgs runArgs;
  private RunStateRecord currentState;
  private RunStateRecord startState;
  private RunStateRecord endState;
  private List<DatasetVersion> inputs;
  private List<DatasetVersion> outputs;

//
//  //Todo: Move json decoration to resource
//  public RunState getState() {
//    if (currentState != null) {
//      return currentState.getState();
//    }
//    return null;
//  }
//  public Optional<Instant> getNominalStartTime() {
//    return nominalStartTime;
//  }
//
//  public Optional<Instant> getNominalEndTime() {
//    return nominalEndTime;
//  }
//
//  public Optional<Instant> getStartedAt() {
//    if (startState != null) {
//      return Optional.ofNullable(startState.getTransitionedAt());
//    }
//    return Optional.empty();
//  }
//
//  public Optional<Instant> getEndedAt() {
//    if (endState != null) {
//      return Optional.ofNullable(endState.getTransitionedAt());
//    }
//    return Optional.empty();
//  }
//
//  public Optional<Long> getDurationMs() {
//    return this.getEndedAt()
//        .flatMap(
//            endedAt -> getStartedAt().map(startedAt -> startedAt.until(endedAt, MILLIS)));
//  }
}
