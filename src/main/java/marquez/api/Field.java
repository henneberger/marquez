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

package marquez.api;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import marquez.common.models.FieldName;
import marquez.common.models.FieldType;
import marquez.common.models.TagName;

@EqualsAndHashCode
@ToString
@Getter
@Builder
class Field {
  private final FieldName name;
  private final FieldType type;
  private final ImmutableSet<TagName> tags;
  private final String description;

  @JsonCreator
  public Field(
      @JsonProperty("name") final String nameAsString,
      @JsonProperty("type") final String typeAsString,
      @JsonProperty("tags") final ImmutableSet<String> tagsAsString,
      final String description) {
    this(
        FieldName.of(nameAsString),
        FieldType.valueOf(typeAsString),
        (tagsAsString == null)
            ? ImmutableSet.of()
            : tagsAsString.stream().map(TagName::of).collect(toImmutableSet()),
        description);
  }

  public Field(
      @NonNull final FieldName name,
      @NonNull final FieldType type,
      @Nullable final ImmutableSet<TagName> tags,
      @Nullable final String description) {
    this.name = name;
    this.type = type;
    this.tags = (tags == null) ? ImmutableSet.of() : tags;
    this.description = description;
  }

  public Optional<String> getDescription() {
    return Optional.ofNullable(description);
  }
}
