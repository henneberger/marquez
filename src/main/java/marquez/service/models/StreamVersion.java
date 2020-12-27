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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class StreamVersion extends DatasetVersion {
  @Getter private final String schemaLocation;

  public StreamVersion(UUID uuid, Instant createdAt, Dataset dataset, UUID version,
      List<DatasetField> fields, Optional<Run> run,
      Namespace namespace, String schemaLocation) {
    super(uuid, createdAt, dataset, version, fields, run, namespace);
    this.schemaLocation = schemaLocation;
  }
}
