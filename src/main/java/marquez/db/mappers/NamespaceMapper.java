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

import static marquez.db.Columns.stringOrNull;
import static marquez.db.Columns.stringOrThrow;
import static marquez.db.Columns.timestampOrThrow;
import static marquez.db.Columns.uuidOrNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.NonNull;
import marquez.common.models.NamespaceName;
import marquez.common.models.OwnerName;
import marquez.db.Columns;
import marquez.service.models.Namespace;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

public final class NamespaceMapper implements RowMapper<Namespace> {
  @Override
  public Namespace map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    //Ownername can be null and should be updated within the same transaction
    String ownerName = stringOrNull(results, Columns.CURRENT_OWNER_NAME);
    if (ownerName == null) {
      return new Namespace(
          uuidOrNull(results, Columns.ROW_UUID),
          NamespaceName.of(stringOrThrow(results, Columns.NAME)),
          timestampOrThrow(results, Columns.CREATED_AT),
          timestampOrThrow(results, Columns.UPDATED_AT),
          stringOrNull(results, Columns.DESCRIPTION));
    } else {
      return new Namespace(
          uuidOrNull(results, Columns.ROW_UUID),
          NamespaceName.of(stringOrThrow(results, Columns.NAME)),
          timestampOrThrow(results, Columns.CREATED_AT),
          timestampOrThrow(results, Columns.UPDATED_AT),
          stringOrNull(results, Columns.CURRENT_OWNER_NAME) == null ? null :
              OwnerName.of(stringOrNull(results, Columns.CURRENT_OWNER_NAME)),
          stringOrNull(results, Columns.DESCRIPTION));
    }
  }
}
