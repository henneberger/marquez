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
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
import marquez.Generator;
import marquez.common.Utils;
import marquez.common.models.DatasetName;
import marquez.common.models.NamespaceName;
import marquez.common.models.SourceName;
import marquez.common.models.SourceType;
import marquez.common.models.TagName;
import marquez.service.models.Dataset;
import marquez.service.models.DatasetField;
import marquez.service.models.DatasetVersion;
import marquez.service.models.JobContext;
import marquez.service.models.Namespace;
import marquez.service.models.Owner;
import marquez.service.models.Run;
import marquez.service.models.Source;
import marquez.service.models.Tag;
import marquez.api.Version;

public final class ModelGenerator extends Generator {
  private ModelGenerator() {}

  public static Namespace newNamespace() {
    return newNamespaceWith(newNamespaceName());
  }

  public static Namespace newNamespaceWith(final NamespaceName name) {
    final Instant now = newTimestamp();
    return new Namespace(
        newRowUuid(), name.getValue(), now, now, Optional.of(newDescription()),
        null, Owner.builder().name(newOwnerName().getValue()).build());
  }

  public static List<Source> newSources(final int limit) {
    return Stream.generate(() -> newSource()).limit(limit).collect(toImmutableList());
  }

  public static Source newSource() {
    return newSourceWith(newSourceName());
  }

  public static Source newSourceWith(final SourceName name) {
    final Instant now = newTimestamp();
    final SourceType type = newSourceType();
    return new Source(
        newRowUuid(),
        type.name(),
        name.getValue(),
        now,
        now,
        newConnectionUrlFor(type).toASCIIString(),
        Optional.of(newDescription()),
        null);
  }

  public static Dataset newDatasetWith(UUID sourceUuid) {
    return newDatasetWith(
        newNamespace().getUuid(),
        sourceUuid,
        toTagUuids(newTagRows(2)),
        newDatasetName());
  }

  public static List<Dataset> newDatasetRowsWith(
      UUID namespaceUuid, UUID sourceUuid, List<UUID> tagUuids, int limit) {
    return Stream.generate(() -> newDatasetWith(namespaceUuid, sourceUuid, tagUuids))
        .limit(limit)
        .collect(toImmutableList());
  }

  public static Dataset newDatasetWith(
      @NonNull final UUID namespaceUuid, @NonNull final UUID sourceUuid, final List<UUID> tagUuids) {
    return newDatasetWith(namespaceUuid, sourceUuid, tagUuids, newDatasetName());
  }

  public static Dataset newDatasetWith(
      final UUID namespaceUuid,
      final UUID sourceUuid,
      final List<UUID> tagUuids,
      final DatasetName name) {
    final Instant now = newTimestamp();
    final DatasetName physicalName = name;
    return new Dataset(
        newRowUuid(),
        newDatasetType().name(),
        name.getValue(),
        physicalName.getValue(),
        now,
        now,
        Optional.of(now),
        Optional.of(newDescription()),
        Source.builder().uuid(sourceUuid).build(),
        null,
        null,
        null,
        Namespace.builder().uuid(namespaceUuid).build(),
        tagUuids.stream().map(t->Tag.builder().uuid(t).build()).collect(Collectors.toList()),
        null);
  }

  public static DatasetVersion newDatasetVersionRowWith(
      UUID datasetUuid, Version version, ImmutableList<UUID> fieldUuids, UUID runUuid) {
    return new DatasetVersion(
        newRowUuid(), newTimestamp(),
        Dataset.builder().uuid(datasetUuid).build(),
        version.getValue(), fieldUuids.stream().map(f-> DatasetField.builder().uuid(f).build()).collect(
        Collectors.toList()), Optional.of(Run.builder().uuid(runUuid).build()), null);
  }

  public static List<Tag> newTagRows(final int limit) {
    return Stream.generate(ModelGenerator::newTagRow).limit(limit).collect(toImmutableList());
  }

  public static Tag newTagRow() {
    return newTagRowWith(newTagName().getValue());
  }

  public static Tag newTagRowWith(final String name) {
    return newTagRowWith(name, null);
  }

  public static Tag newTagRowWith(final String name, final String description) {
    final Instant now = newTimestamp();
    return new Tag(newRowUuid(),name, now, now, Optional.of(newDescription()), null, null);
  }

  public static List<UUID> toTagUuids(final List<Tag> rows) {
    return rows.stream().map(Tag::getUuid).collect(toImmutableList());
  }

  public static List<JobContext> newJobContextRows(final int limit) {
    return Stream.generate(ModelGenerator::newJobContextRow).limit(limit).collect(toImmutableList());
  }

  public static JobContext newJobContextRow() {
    return newJobContextRowWith(newContext());
  }

  public static JobContext newJobContextRowWith(final Map<String, String> context) {
    return new JobContext(newRowUuid(), newTimestamp(), Utils.toJson(context), Utils.checksumFor(context), null);
  }

  public static UUID newRowUuid() {
    return UUID.randomUUID();
  }
}
