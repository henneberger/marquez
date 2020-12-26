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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static marquez.common.models.ModelGenerator.newConnectionUrlFor;
import static marquez.common.models.ModelGenerator.newContext;
import static marquez.common.models.ModelGenerator.newDatasetName;
import static marquez.common.models.ModelGenerator.newDatasetType;
import static marquez.common.models.ModelGenerator.newDescription;
import static marquez.common.models.ModelGenerator.newNamespaceName;
import static marquez.common.models.ModelGenerator.newOwnerName;
import static marquez.common.models.ModelGenerator.newSourceName;
import static marquez.common.models.ModelGenerator.newSourceType;
import static marquez.common.models.ModelGenerator.newTagName;

import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.NonNull;
import marquez.Generator;
import marquez.common.Utils;
import marquez.common.models.DatasetName;
import marquez.common.models.NamespaceName;
import marquez.common.models.SourceName;
import marquez.common.models.SourceType;
import marquez.common.models.TagName;
import marquez.service.models.Namespace;
import marquez.service.models.Source;
import marquez.service.models.Tag;
import marquez.service.models.Version;

public final class ModelGenerator extends Generator {
  private ModelGenerator() {}

  public static Namespace newNamespaceRow() {
    return newNamespaceRowWith(newNamespaceName());
  }

  public static Namespace newNamespaceRowWith(final NamespaceName name) {
    final Instant now = newTimestamp();
    return new Namespace(
        newRowUuid(), name, now, now, newOwnerName(), newDescription(), null);
  }

  public static List<SourceRow> newSourceRows(final int limit) {
    return Stream.generate(() -> newSourceRow()).limit(limit).collect(toImmutableList());
  }

  public static SourceRow newSourceRow() {
    return newSourceRowWith(newSourceName());
  }

  public static SourceRow newSourceRowWith(final SourceName name) {
    final Instant now = newTimestamp();
    final SourceType type = newSourceType();
    return new SourceRow(
        type.name(),
        now,
        now,
        name.getValue(),
        newConnectionUrlFor(type).toASCIIString(),
        newDescription());
  }

  public static Source newSourceWith(final SourceName name) {
    final Instant now = newTimestamp();
    final SourceType type = newSourceType();
    return new Source(
        newRowUuid(),
        type,
        name,
        now,
        now,
        newConnectionUrlFor(type),
        newDescription());
  }

  public static DatasetRow newDatasetRowWith(UUID sourceUuid) {
    return newDatasetRowWith(
        newNamespaceRow().getUuid(),
        sourceUuid,
        toTagUuids(newTagRows(2)),
        newDatasetName());
  }

  public static List<DatasetRow> newDatasetRowsWith(
      UUID namespaceUuid, UUID sourceUuid, List<UUID> tagUuids, int limit) {
    return Stream.generate(() -> newDatasetRowWith(namespaceUuid, sourceUuid, tagUuids))
        .limit(limit)
        .collect(toImmutableList());
  }

  public static DatasetRow newDatasetRowWith(
      @NonNull final UUID namespaceUuid, @NonNull final UUID sourceUuid, final List<UUID> tagUuids) {
    return newDatasetRowWith(namespaceUuid, sourceUuid, tagUuids, newDatasetName());
  }

  public static DatasetRow newDatasetRowWith(
      final UUID namespaceUuid,
      final UUID sourceUuid,
      final List<UUID> tagUuids,
      final DatasetName name) {
    final Instant now = newTimestamp();
    final DatasetName physicalName = name;
    return new DatasetRow(
        newRowUuid(),
        newDatasetType().name(),
        now,
        now,
        namespaceUuid,
        sourceUuid,
        name.getValue(),
        physicalName.getValue(),
        tagUuids,
        null,
        newDescription(),
        null);
  }

  public static DatasetVersionRow newDatasetVersionRowWith(
      UUID datasetUuid, Version version, ImmutableList<UUID> fieldUuids, UUID runUuid) {
    return new DatasetVersionRow(
        newRowUuid(), newTimestamp(), datasetUuid, version.getValue(), fieldUuids, runUuid, null);
  }

  public static List<Tag> newTagRows(final int limit) {
    return Stream.generate(ModelGenerator::newTagRow).limit(limit).collect(toImmutableList());
  }

  public static Tag newTagRow() {
    return newTagRowWith(newTagName().getValue());
  }

  public static Tag newTagRowWith(final String name) {
    final Instant now = newTimestamp();
    return new Tag(newRowUuid(), now, now, TagName.of(name), newDescription());
  }

  public static Tag newTagRowWith(final String name, final String description) {
    final Instant now = newTimestamp();
    return new Tag(newRowUuid(), now, now, TagName.of(name), description);
  }

  public static List<UUID> toTagUuids(final List<Tag> rows) {
    return rows.stream().map(Tag::getUuid).collect(toImmutableList());
  }

  public static List<JobContextRow> newJobContextRows(final int limit) {
    return Stream.generate(ModelGenerator::newJobContextRow).limit(limit).collect(toImmutableList());
  }

  public static JobContextRow newJobContextRow() {
    return newJobContextRowWith(newContext());
  }

  public static JobContextRow newJobContextRowWith(final Map<String, String> context) {
    return new JobContextRow(
        newRowUuid(), newTimestamp(), Utils.toJson(context), Utils.checksumFor(context));
  }

  public static UUID newRowUuid() {
    return UUID.randomUUID();
  }
}
