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

package marquez.service;

import static com.google.common.base.Preconditions.checkArgument;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.db.DatasetDao;
import marquez.db.DatasetFieldDao;
import marquez.db.DatasetVersionDao;
import marquez.db.TagDao;
import marquez.service.input.DatasetInputFragment;
import marquez.service.input.DatasetInputFragment.DatasetInputFragmentBuilder;
import marquez.service.input.DatasetInputFragment.DatasetVersionFragment;
import marquez.service.input.DatasetInputFragment.DatasetVersionIdFragment;
import marquez.service.input.DatasetInputFragment.TagFragment;
import marquez.service.input.DatasetServiceFragment;
import marquez.service.models.Dataset;
import marquez.service.models.DatasetField;
import marquez.service.models.DatasetVersion;
import marquez.service.models.Tag;

@Slf4j
@AllArgsConstructor
public class DatasetService implements ServiceMetrics {
  private final DatasetDao datasetDao;
  private final DatasetFieldDao fieldDao;
  private final TagDao tagDao;
  private final DatasetVersionDao datasetVersionDao;

  public Dataset createOrUpdate(
      @NonNull String namespace,
      @NonNull String datasetName,
      @NonNull DatasetServiceFragment fragment) {
    DatasetInputFragmentBuilder inputFragment = DatasetInputFragment.builder()
        .now(Instant.now())
        .fragment(fragment);

    Optional<DatasetVersion> version = datasetVersionDao.findByVersion(fragment.getVersion());
    if (version.isPresent()) {
      inputFragment.datasetVersionIdFragment(
          Optional.of(
          DatasetVersionIdFragment.builder()
              .uuid(version.get().getUuid())
              .build()));
    }

    inputFragment.tags(filterExistingTags(fragment.getTagFragments()));

    Dataset dataset = datasetDao.upsert(inputFragment.build());
    log.info(
        "Successfully created dataset '{}' for namespace '{}' with meta: {}",
        datasetName,
        namespace,
        fragment);
    datasets.labels(namespace, fragment.getType()).inc();
    return dataset;
  }

  private List<TagFragment> filterExistingTags(
      List<DatasetServiceFragment.TagFragment> tagFragments) {
    return tagDao.findAllIn(
        tagFragments.stream().map(DatasetServiceFragment.TagFragment::getName).collect(Collectors.toList())).stream().map(t->
        DatasetInputFragment.TagFragment.builder()
        .name(t.getName()).uuid(t.getUuid()).build())
        .collect(Collectors.toList());
  }

  public boolean exists(@NonNull String namespace, @NonNull String datasetName) {
    return datasetDao.exists(namespace, datasetName);
  }

  public boolean fieldExists(@NonNull String namespace, @NonNull String datasetName, @NonNull String fieldName) {
    return fieldDao.exists(namespace, datasetName, fieldName);
  }

  public Optional<Dataset> get(@NonNull String namespace, @NonNull String datasetName) {
    return datasetDao.find(namespace, datasetName);
  }
//
//  public Optional<Dataset> getBy(@NonNull String namespace, String datasetName, UUID versionUuid) {
//    return datasetDao.find(namespace, datasetName, versionUuid);
//  }

  public List<Dataset> getAll(@NonNull String namespace, int limit, int offset) {
    checkArgument(limit >= 0, "limit must be >= 0");
    checkArgument(offset >= 0, "offset must be >= 0");
    return datasetDao.findAll(namespace, limit, offset);
  }

  public Dataset tagWith(@NonNull String namespace, @NonNull String datasetName, @NonNull String tagName) {
    datasetDao.updateTags(namespace, datasetName, tagName);
    log.info("Successfully tagged dataset '{}' with '{}'.", datasetName, tagName);
    return get(namespace, datasetName).get();
  }

  public Dataset tagFieldWith(@NonNull String namespace, @NonNull String datasetName, @NonNull String fieldName, @NonNull String tagName) {
      final Dataset datasetRow =
          datasetDao.find(namespace, datasetName).get();
      final DatasetField fieldRow =
          fieldDao.find(datasetRow.getUuid(), fieldName).get();
      final Tag tagRow =
          tagDao.findBy(tagName.toUpperCase(Locale.getDefault())).get();
      final Instant taggedAt = Instant.now();
      fieldDao.updateTags(fieldRow.getUuid(), tagRow.getUuid(), taggedAt);
      log.info(
          "Successfully tagged field '{}' for dataset '{}' with '{}'.",
          fieldName,
          datasetName,
          tagName);
      return get(namespace, datasetName).get();
  }
}
