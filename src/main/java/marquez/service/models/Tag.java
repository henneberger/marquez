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

import static com.fasterxml.jackson.annotation.JsonProperty.Access.READ_ONLY;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import marquez.common.models.TagName;

@EqualsAndHashCode
@ToString
@Getter
public final class Tag {
  @JsonUnwrapped
  @JsonProperty(access = READ_ONLY)
  private TagName name;
  @JsonIgnore
  private UUID uuid;
  @JsonIgnore
  private Instant createdAt;
  @JsonIgnore
  private Instant updatedAt;

  @Nullable String description;

  @JsonCreator
  public Tag(
      @JsonProperty("name") @NonNull final String nameAsString,
      @JsonProperty("description") @Nullable final String description) {
    this(TagName.of(nameAsString), description);
  }

  public Tag(@NonNull final TagName name, @Nullable final String description) {
    this.name = name;
    this.description = description;
  }

  public Tag(UUID uuid, Instant createdAt, Instant updatedAt,
      TagName name, String description) {
    this.uuid = uuid;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
    this.name = name;
    this.description = description;
  }

  public Optional<String> getDescription() {
    return Optional.ofNullable(description);
  }
}
