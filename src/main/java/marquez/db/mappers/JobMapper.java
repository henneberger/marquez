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

package marquez.db.mappers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import marquez.db.Columns;
import marquez.service.models.Job;
import org.jdbi.v3.core.statement.StatementContext;

public final class JobMapper extends AbstractMapper<Job> {
  @Override
  public Job map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());
    return new Job(
        uuidOrThrow(results, Columns.ROW_UUID, columnNames),
        stringOrThrow(results, Columns.TYPE, columnNames),
        stringOrThrow(results, Columns.NAME, columnNames),
        timestampOrThrow(results, Columns.CREATED_AT, columnNames),
        timestampOrThrow(results, Columns.UPDATED_AT, columnNames),
        Optional.ofNullable(stringOrNull(results, Columns.DESCRIPTION, columnNames)),
        null,
        toNamespaceLink(uuidOrThrow(results, Columns.NAMESPACE_UUID, columnNames), stringOrThrow(results, Columns.NAMESPACE_NAME, columnNames)),
        toJobVersionLink(uuidOrNull(results, Columns.CURRENT_VERSION_UUID, columnNames)));
  }
}
