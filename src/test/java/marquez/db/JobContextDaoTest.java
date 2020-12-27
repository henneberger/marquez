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
public class JobContextDaoTest {
//
//  @ClassRule public static final JdbiRule dbRule = JdbiRuleInit.init();
//
//  private static JobContextDao jobContextDao;
//
//  @BeforeClass
//  public static void setUpOnce() {
//    final Jdbi jdbi = dbRule.getJdbi();
//    jobContextDao = jdbi.onDemand(JobContextDao.class);
//  }
//
//  @Test
//  public void testInsert() {
//    final int rowsBefore = jobContextDao.count();
//
//    final JobContextRow newRow = newJobContextRow();
//    jobContextDao.insert(newRow);
//
//    final int rowsAfter = jobContextDao.count();
//    assertThat(rowsAfter).isEqualTo(rowsBefore + 1);
//  }
//
//  @Test
//  public void testExists() {
//    final Map<String, String> context = newContext();
//    final JobContextRow newRow = newJobContextRowWith(context);
//    jobContextDao.insert(newRow);
//
//    final String checksum = Utils.checksumFor(context);
//    final boolean exists = jobContextDao.exists(checksum);
//    assertThat(exists).isTrue();
//  }
//
//  @Test
//  public void testFindBy_uuid() {
//    final JobContextRow newRow = newJobContextRow();
//    jobContextDao.insert(newRow);
//
//    final Optional<JobContextRow> row = jobContextDao.findBy(newRow.getUuid());
//    assertThat(row).isPresent();
//  }
//
//  @Test
//  public void testFindBy_uuidNotFound() {
//    final JobContextRow newRow = newJobContextRow();
//
//    final Optional<JobContextRow> row = jobContextDao.findBy(newRow.getUuid());
//    assertThat(row).isNotPresent();
//  }
//
//  @Test
//  public void testFindBy_checksum() {
//    final Map<String, String> context = newContext();
//    final JobContextRow newRow = newJobContextRowWith(context);
//    jobContextDao.insert(newRow);
//
//    final String checksum = Utils.checksumFor(context);
//    final Optional<JobContextRow> row = jobContextDao.findBy(checksum);
//    assertThat(row).isPresent();
//  }
//
//  @Test
//  public void testFindBy_checksumNotFound() {
//    final Map<String, String> context = newContext();
//    final JobContextRow newRow = newJobContextRowWith(context);
//
//    final String checksum = Utils.checksumFor(context);
//    final Optional<JobContextRow> row = jobContextDao.findBy(checksum);
//    assertThat(row).isNotPresent();
//  }
//
//  @Test
//  public void testFindAll() {
//    final List<JobContextRow> newRows = newJobContextRows(4);
//    newRows.forEach(newRow -> jobContextDao.insert(newRow));
//
//    final List<JobContextRow> rows = jobContextDao.findAll(4, 0);
//    assertThat(rows).isNotNull().hasSize(4);
//  }
}
