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

package marquez.db;

import marquez.DataAccessTests;
import marquez.IntegrationTests;
import org.junit.experimental.categories.Category;

@Category({DataAccessTests.class, IntegrationTests.class})
public class DatasetDaoTest {

//  @ClassRule public static final JdbiRule dbRule = JdbiRuleInit.init();
//
//  private static final NamespaceName NAMESPACE_NAME = newNamespaceName();
//
//  private static List<Function<Dataset, ? extends Object>> DATASET_ROW_GETTERS =
//      asList(
//          Dataset::getUuid,
//          Dataset::getType,
//          Dataset::getName,
//          Dataset::getCreatedAt,
//          Dataset::getUpdatedAt,
//          Dataset::getNamespaceUuid,
//          Dataset::getSourceUuid,
//          Dataset::getName,
//          Dataset::getPhysicalName,
//          Dataset::getTagUuids,
//          Dataset::getLastModifiedAt,
//          Dataset::getDescription,
//          Dataset::getCurrentVersionUuid);
//  private Source source;
//  private UUID namespaceUuid;
//
//  private static void assertEquals(Dataset actual, Dataset expected) {
//    for (Function<Dataset, ? extends Object> getter : DATASET_ROW_GETTERS) {
//      assertThat(getter.apply(actual)).isEqualTo(getter.apply(expected));
//    }
//  }
//
//  private static NamespaceDao namespaceDao;
//  private static SourceDao sourceDao;
//  private static DatasetDao datasetDao;
//  private static DatasetVersionDao datasetVersionDao;
//  private static TagDao tagDao;
//
//  private static Namespace namespaceRow;
//  private static List<Tag> tagRows;
//
//  @Before
//  public void before() {
//    final Jdbi jdbi = dbRule.getJdbi();
//    namespaceDao = jdbi.onDemand(NamespaceDao.class);
//    sourceDao = jdbi.onDemand(SourceDao.class);
//    datasetDao = jdbi.onDemand(DatasetDao.class);
//    datasetVersionDao = jdbi.onDemand(DatasetVersionDao.class);
//    tagDao = jdbi.onDemand(TagDao.class);
//
//    namespaceRow = newNamespaceWith(NAMESPACE_NAME);
//    namespaceRow = namespaceDao.upsert(UpsertNamespaceFragment.build(namespaceRow));
//    namespaceUuid = namespaceRow.getUuid();
//    source = sourceDao.upsert(newSource());
//
//    tagRows = newTagRows(2).stream()
//        .map(tagRow -> tagDao.upsert(TagDao.UpsertTagFragment.build(tagRow)))
//        .collect(Collectors.toList());
//  }
//
//  @Test
//  public void testInsert() {
//    final int rowsBefore = datasetDao.count();
//
//    final Dataset newRow =
//        ModelGenerator.newDatasetWith(namespaceUuid, source.getUuid(), toTagUuids(tagRows));
//    datasetDao.insert(newRow);
//
//    final int rowsAfter = datasetDao.count();
//    assertThat(rowsAfter).isEqualTo(rowsBefore + 1);
//  }
//
//  @Test
//  public void testExists() {
//    final Dataset newRow =
//        ModelGenerator.newDatasetWith(namespaceUuid, source.getUuid(), toTagUuids(tagRows));
//    datasetDao.insert(newRow);
//
//    final boolean exists = datasetDao.exists(NAMESPACE_NAME.getValue(), newRow.getName());
//    assertThat(exists).isTrue();
//  }
//
//  @Test
//  public void testUpdateTags() {
//    final Dataset newRow =
//        ModelGenerator.newDatasetWith(namespaceUuid, source.getUuid(), toTagUuids(tagRows));
//    datasetDao.insert(newRow);
//
//    // Tag
//    final Tag newTagRow = tagDao.upsert(TagDao.UpsertTagFragment.build(newTagRow()));
//
//    final Instant taggedAt = newTimestamp();
//    datasetDao.updateTags(newRow.getUuid(), newTagRow.getUuid(), taggedAt);
//
//    final ExtendedDataset row = datasetDao.findBy(newRow.getUuid()).get();
//    assertThat(row).isNotNull();
//    assertThat(row.getTagUuids()).contains(newTagRow.getUuid());
//  }
//
//  @Test
//  public void testUpdateLastModifiedAt() {
//    final Dataset newRow =
//        ModelGenerator.newDatasetWith(namespaceUuid, source.getUuid(), toTagUuids(tagRows));
//    datasetDao.insert(newRow);
//
//    // Modified
//    final Instant lastModifiedAt = newTimestamp();
//    datasetDao.updateLastModifiedAt(Lists.newArrayList(newRow.getUuid()), lastModifiedAt);
//
//    final ExtendedDataset row = datasetDao.findBy(newRow.getUuid()).get();
//    assertThat(row.getLastModifiedAt()).isPresent().hasValue(lastModifiedAt);
//  }
//
//  @Test
//  public void testFindBy() {
//    final Dataset newRow =
//        ModelGenerator.newDatasetWith(namespaceUuid, source.getUuid(), toTagUuids(tagRows));
//    datasetDao.insert(newRow);
//
//    final Optional<ExtendedDataset> row = datasetDao.findBy(newRow.getUuid());
//    assertThat(row).isPresent();
//    assertEquals(newRow, row.get());
//  }
//
//  @Test
//  public void testFindBy_notFound() {
//    final Dataset newRow = ModelGenerator.newDatasetWith(source.getUuid());
//
//    final Optional<ExtendedDataset> row = datasetDao.findBy(newRow.getUuid());
//    assertThat(row).isEmpty();
//  }
//
//  @Test
//  public void testFind() {
//    final Dataset newRow =
//        ModelGenerator.newDatasetWith(namespaceUuid, source.getUuid(), toTagUuids(tagRows));
//    datasetDao.insert(newRow);
//
//    final Optional<ExtendedDataset> row =
//        datasetDao.find(NAMESPACE_NAME.getValue(), newRow.getName());
//    assertThat(row).isPresent();
//    ExtendedDataset dsRow = row.get();
//    assertEquals(newRow, dsRow);
//  }
//
//  @Test
//  public void testFind_notFound() {
//    final NamespaceName namespaceName = newNamespaceName();
//    final DatasetName datasetName = newDatasetName();
//
//    final Optional<ExtendedDataset> row =
//        datasetDao.find(namespaceName.getValue(), datasetName.getValue());
//    assertThat(row).isEmpty();
//  }
//
//  @Test
//  public void testFindAllIn_uuidList() {
//    final List<Dataset> newRows =
//        newDatasetsWith(namespaceUuid, source.getUuid(), toTagUuids(tagRows), 4);
//    newRows.forEach(newRow -> datasetDao.insert(newRow));
//
//    final List<UUID> newRowUuids =
//        newRows.stream().map(newRow -> newRow.getUuid()).collect(toImmutableList());
//
//    final List<ExtendedDataset> rows = datasetDao.findAllIn(newRowUuids);
//    assertThat(rows).hasSize(4);
//
//    final List<UUID> rowUuids = rows.stream().map(row -> row.getUuid()).collect(toImmutableList());
//    assertThat(rowUuids).containsAll(newRowUuids);
//  }
//
//  @Test
//  public void testFindAllIn_stringList() {
//    final List<Dataset> newRows =
//        newDatasetsWith(namespaceUuid, source.getUuid(), toTagUuids(tagRows), 4);
//    newRows.forEach(newRow -> datasetDao.insert(newRow));
//
//    final List<String> newDatasetNames =
//        newRows.stream().map(newRow -> newRow.getName()).collect(toImmutableList());
//
//    final List<Dataset> rows = datasetDao.findAllIn(NAMESPACE_NAME.getValue(), newDatasetNames);
//    assertThat(rows).hasSize(4);
//
//    final List<String> datasetNames =
//        rows.stream().map(row -> row.getName()).collect(toImmutableList());
//    assertThat(datasetNames).containsAll(newDatasetNames);
//
//    List<ExtendedDataset> findAllExtendedIn =
//        datasetDao.findAllIn(rows.stream().map(Dataset::getUuid).collect(toImmutableList()));
//
//    assertThat(
//            findAllExtendedIn.stream().map(ExtendedDataset::getName).collect(toImmutableList()))
//        .containsAll(newDatasetNames);
//  }
//
//  @Test
//  public void testFindAll() {
//    final List<Dataset> newRows =
//        newDatasetsWith(namespaceUuid, source.getUuid(), toTagUuids(tagRows), 4);
//    newRows.forEach(newRow -> datasetDao.insert(newRow));
//
//    final List<ExtendedDataset> rows = datasetDao.findAll(NAMESPACE_NAME.getValue(), 4, 0);
//    assertThat(rows).isNotNull().hasSize(4);
//  }
//
//  @Test
//  public void testDatasetVersions() {
//    final Dataset ds =
//        ModelGenerator.newDatasetWith(namespaceUuid, source.getUuid(), toTagUuids(tagRows));
//    datasetDao.insert(ds);
//
//    DatasetVersion dsv =
//        newDatasetVersionWith(ds.getUuid(), newVersion(), ImmutableList.of(), newRowUuid());
//
//    datasetVersionDao.insert(dsv);
//
//    final List<ExtendedDatasetVersion> rows =
//        datasetVersionDao.findByRunId(dsv.getRunUuid().get());
//    assertThat(rows).isNotNull().hasSize(1);
//  }
}
