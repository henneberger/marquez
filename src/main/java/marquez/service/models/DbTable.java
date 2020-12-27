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
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public final class DbTable extends Dataset {
  public DbTable(UUID uuid, String type, String name, String physicalName,
      Instant createdAt, Instant updatedAt, Optional<Instant> lastModifiedAt,
      Optional<String> description, Source source,
      List<DatasetField> fields, List<JobVersion> jobVersionAsInput,
      List<JobVersion> jobVersionAsOutput, Namespace namespace,
      List<Tag> tags, DatasetVersion currentVersion) {
    super(uuid, type, name, physicalName, createdAt, updatedAt, lastModifiedAt, description, source,
        fields, jobVersionAsInput, jobVersionAsOutput, namespace, tags, currentVersion);
  }
}
