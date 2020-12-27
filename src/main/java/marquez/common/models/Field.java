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

package marquez.common.models;

import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import marquez.service.models.Tag;

@EqualsAndHashCode
@ToString
@Getter
@AllArgsConstructor
@Builder
//todo Move to resource
public final class Field {
  private final String name;
  private final String type;
  private final List<Tag> tags;
  private final Optional<String> description;
//
//  @JsonCreator
//  public Field(
//      @JsonProperty("name") final String nameAsString,
//      @JsonProperty("type") final String typeAsString,
//      @JsonProperty("tags") final ImmutableSet<String> tagsAsString,
//      final String description) {
//    this(
//        FieldName.of(nameAsString),
//        FieldType.valueOf(typeAsString),
//        (tagsAsString == null)
//            ? ImmutableSet.of()
//            : tagsAsString.stream().map(TagName::of).collect(toImmutableSet()),
//        description);
//  }
//
//  public Field(
//      @NonNull final FieldName name,
//      @NonNull final FieldType type,
//      @Nullable final ImmutableSet<TagName> tags,
//      @Nullable final String description) {
//    this.name = name;
//    this.type = type;
//    this.tags = (tags == null) ? ImmutableSet.of() : tags;
//    this.description = description;
//  }
//
//  public Optional<String> getDescription() {
//    return Optional.ofNullable(description);
//  }
}
