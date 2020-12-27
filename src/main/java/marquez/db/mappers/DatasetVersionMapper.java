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
import java.util.Set;
import lombok.NonNull;
import marquez.db.Columns;
import marquez.service.models.DatasetVersion;
import org.jdbi.v3.core.statement.StatementContext;

public final class DatasetVersionMapper extends AbstractMapper<DatasetVersion> {
  @Override
  public DatasetVersion map(
      @NonNull ResultSet results, @NonNull StatementContext context) throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());

    return new DatasetVersion(
        uuidOrThrow(results, Columns.ROW_UUID, columnNames),
        timestampOrThrow(results, Columns.CREATED_AT, columnNames),
        toDatasetLink(uuidOrThrow(results, Columns.DATASET_UUID, columnNames)),
        uuidOrThrow(results, Columns.VERSION, columnNames),
        toDatasetFieldsLink(uuidArrayOrNull(results, Columns.FIELD_UUIDS, columnNames)),
        toRunLink(uuidOrNull(results, Columns.RUN_UUID, columnNames)),
        toNamespaceLink(uuidOrNull(results, Columns.NAMESPACE_UUID, columnNames), stringOrNull(results, Columns.NAMESPACE_NAME, columnNames))
      );
  }

}
