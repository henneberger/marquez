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

import static marquez.common.models.ModelGenerator.newSourceName;
import static marquez.db.models.ModelGenerator.newSourceRow;
import static marquez.db.models.ModelGenerator.newSourceRows;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import marquez.DataAccessTests;
import marquez.IntegrationTests;
import marquez.JdbiRuleInit;
import marquez.service.models.SourceRow;
import marquez.service.models.Source;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.testing.JdbiRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({DataAccessTests.class, IntegrationTests.class})
public class SourceDaoTest {

  @ClassRule public static final JdbiRule dbRule = JdbiRuleInit.init();

  private static SourceDao sourceDao;

  @BeforeClass
  public static void setUpOnce() {
    final Jdbi jdbi = dbRule.getJdbi();
    sourceDao = jdbi.onDemand(SourceDao.class);
  }

  @Test
  public void testInsert() {
    final int rowsBefore = sourceDao.count();

    final SourceRow newRow = newSourceRow();
    Source source = sourceDao.upsert(newRow);
    assertThat(source).isNotNull();
    final int rowsAfter = sourceDao.count();
    assertThat(rowsAfter).isEqualTo(rowsBefore + 1);
  }

  @Test
  public void testFindBy_name() {
    final SourceRow newRow = newSourceRow();
    sourceDao.upsert(newRow);

    final Optional<Source> row = sourceDao.findBy(newRow.getName());
    assertThat(row).isPresent();
  }

  @Test
  public void testFindBy_nameNotFound() {
    final Optional<Source> row = sourceDao.findBy(newSourceName().getValue());
    assertThat(row).isEmpty();
  }

  @Test
  public void testFindAll() {
    final List<SourceRow> newRows = newSourceRows(4);
    newRows.forEach(newRow -> sourceDao.upsert(newRow));

    final List<Source> rows = sourceDao.findAll(4, 0);
    assertThat(rows).isNotNull().hasSize(4);
  }
}
