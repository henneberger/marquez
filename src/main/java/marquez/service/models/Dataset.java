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

//import marquez.common.models.DatasetType;
//
//@EqualsAndHashCode
//@ToString
//@JsonTypeInfo(
//    use = JsonTypeInfo.Id.NAME,
//    include = JsonTypeInfo.As.EXISTING_PROPERTY,
//    property = "type")
//@JsonSubTypes({
//  @JsonSubTypes.Type(value = DbTable.class, name = "DB_TABLE"),
//  @JsonSubTypes.Type(value = Stream.class, name = "STREAM")
//})
@AllArgsConstructor
@Getter
@Setter
@ToString
@Builder
public class Dataset {
  private final UUID uuid;
  private String type;
  private String name;
  private String physicalName;
  private Instant createdAt;
  private Instant updatedAt;
  private Optional<Instant> lastModifiedAt;
  private Optional<String> description;
  private Source source;
  private List<DatasetField> fields;
  private List<JobVersion> jobVersionAsInput;
  private List<JobVersion> jobVersionAsOutput;
  private Namespace namespace;
  private List<Tag> tags;
  private DatasetVersion currentVersion;
}
