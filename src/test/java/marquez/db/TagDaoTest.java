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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.toArray;
import static marquez.common.models.ModelGenerator.newTagName;
import static marquez.db.models.ModelGenerator.newRowUuid;
import static marquez.db.models.ModelGenerator.newTagRow;
import static marquez.db.models.ModelGenerator.newTagRows;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import marquez.DataAccessTests;
import marquez.IntegrationTests;
import marquez.JdbiRuleInit;
import marquez.service.models.Tag;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.testing.JdbiRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({DataAccessTests.class, IntegrationTests.class})
public class TagDaoTest {

  @ClassRule public static final JdbiRule dbRule = JdbiRuleInit.init();

  private static TagDao tagDao;

  @BeforeClass
  public static void setUpOnce() {
    final Jdbi jdbi = dbRule.getJdbi();
    tagDao = jdbi.onDemand(TagDao.class);
  }

  @Test
  public void testInsert() {
    final int rowsBefore = tagDao.count();

    final Tag newRow = newTagRow();
    tagDao.upsert(TagDao.UpsertTagFragment.build(newRow));

    final int rowsAfter = tagDao.count();
    assertThat(rowsAfter).isEqualTo(rowsBefore + 1);
  }

  @Test
  public void testExists() {
    final Tag newRow = newTagRow();
    tagDao.upsert(TagDao.UpsertTagFragment.build(newRow));

    final boolean exists = tagDao.exists(newRow.getName().getValue());
    assertThat(exists).isTrue();
  }

  @Test
  public void testFindBy_uuid() {
    final Tag newRow = tagDao.upsert(TagDao.UpsertTagFragment.build(newTagRow()));

    final Optional<Tag> row = tagDao.findBy(newRow.getUuid());
    assertThat(row).isPresent();
  }

  @Test
  public void testFindBy_uuidNotFound() {
    final Optional<Tag> row = tagDao.findBy(newRowUuid());
    assertThat(row).isEmpty();
  }

  @Test
  public void testFindBy_name() {
    final Tag newRow = newTagRow();
    tagDao.upsert(TagDao.UpsertTagFragment.build(newRow));

    final Optional<Tag> row = tagDao.findBy(newRow.getName().getValue());
    assertThat(row).isPresent();
  }

  @Test
  public void testFindBy_nameNotFound() {
    final Optional<Tag> row = tagDao.findBy(newTagName().getValue());
    assertThat(row).isEmpty();
  }

  @Test
  public void testFindAllIn_uuidList() {
    final List<Tag> newRows = newTagRows(4).stream()
        .map(newRow -> tagDao.upsert(TagDao.UpsertTagFragment.build(newRow)))
      .collect(Collectors.toList());

    final List<UUID> newRowUuids =
        newRows.stream().map(Tag::getUuid).collect(toImmutableList());

    final List<Tag> rows = tagDao.findAllIn(toArray(newRowUuids, UUID.class));
    assertThat(rows).hasSize(4);

    final List<UUID> rowUuids = rows.stream().map(Tag::getUuid).collect(toImmutableList());
    assertThat(rowUuids).containsAll(newRowUuids);
  }

  @Test
  public void testFindAllIn_stringList() {
    final List<Tag> newRows = newTagRows(4).stream()
        .map(newRow -> tagDao.upsert(TagDao.UpsertTagFragment.build(newRow)))
        .collect(Collectors.toList());

    final List<String> newTagNames =
        newRows.stream().map(newRow -> newRow.getName().getValue()).collect(toImmutableList());

    final List<Tag> rows = tagDao.findAllIn(toArray(newTagNames, String.class));
    assertThat(rows).hasSize(4);

    final List<String> tagNames =
        rows.stream().map(row -> row.getName().getValue()).collect(toImmutableList());
    assertThat(tagNames).containsAll(newTagNames);
  }

  @Test
  public void testFindAll() {
    final List<Tag> newRows = newTagRows(4).stream()
        .map(newRow -> tagDao.upsert(TagDao.UpsertTagFragment.build(newRow)))
        .collect(Collectors.toList());

    final List<Tag> rows = tagDao.findAll(4, 0);
    assertThat(rows).isNotNull().hasSize(4);
  }
}
