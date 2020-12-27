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
import marquez.common.models.DatasetType;
import marquez.db.Columns;
import marquez.service.models.Dataset;
import marquez.service.models.DbTable;
import marquez.service.models.Stream;
import org.jdbi.v3.core.statement.StatementContext;

public final class DatasetMapper extends AbstractMapper<Dataset> {
  @Override
  public Dataset map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    Set<String> columnNames = getColumnNames(results.getMetaData());
    String type = stringOrThrow(results, Columns.TYPE, columnNames);

    if (type.equals(DatasetType.STREAM.name())) {
      return new Stream(
          uuidOrThrow(results, Columns.ROW_UUID, columnNames),
          type,
          stringOrThrow(results, Columns.NAME, columnNames),
          stringOrThrow(results, Columns.PHYSICAL_NAME, columnNames),
          timestampOrThrow(results, Columns.CREATED_AT, columnNames),
          timestampOrThrow(results, Columns.UPDATED_AT, columnNames),
          Optional.ofNullable(timestampOrNull(results, Columns.LAST_MODIFIED_AT, columnNames)),
          Optional.ofNullable(stringOrNull(results, Columns.DESCRIPTION, columnNames)),
          toSourceLink(uuidOrThrow(results, Columns.SOURCE_UUID, columnNames), stringOrThrow(results, Columns.SOURCE_NAME, columnNames)),
          null,
          null,
          null,
          toNamespaceLink(uuidOrThrow(results, Columns.NAMESPACE_UUID, columnNames), stringOrThrow(results, Columns.NAMESPACE_NAME, columnNames)),
          toTagsLink(uuidArrayOrThrow(results, Columns.TAG_UUIDS, columnNames)),
          toDatasetVersionLink(uuidOrNull(results, Columns.CURRENT_VERSION_UUID, columnNames)),
          stringOrNull(results, Columns.LOCATION, columnNames)
        );
    }
    return new DbTable(
        uuidOrThrow(results, Columns.ROW_UUID, columnNames),
        type,
        stringOrThrow(results, Columns.NAME, columnNames),
        stringOrThrow(results, Columns.PHYSICAL_NAME, columnNames),
        timestampOrThrow(results, Columns.CREATED_AT, columnNames),
        timestampOrThrow(results, Columns.UPDATED_AT, columnNames),
        Optional.ofNullable(timestampOrNull(results, Columns.LAST_MODIFIED_AT, columnNames)),
        Optional.ofNullable(stringOrNull(results, Columns.DESCRIPTION, columnNames)),
        toSourceLink(uuidOrThrow(results, Columns.SOURCE_UUID, columnNames), stringOrThrow(results, Columns.SOURCE_NAME, columnNames)),
        null,
        null,
        null,
        toNamespaceLink(uuidOrThrow(results, Columns.NAMESPACE_UUID, columnNames), stringOrThrow(results, Columns.NAMESPACE_NAME, columnNames)),
        toTagsLink(uuidArrayOrThrow(results, Columns.TAG_UUIDS, columnNames)),
        toDatasetVersionLink(uuidOrNull(results, Columns.CURRENT_VERSION_UUID, columnNames)));
  }
}
