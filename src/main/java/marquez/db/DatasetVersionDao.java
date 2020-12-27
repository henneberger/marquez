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

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.NonNull;
import marquez.db.mappers.DatasetVersionMapper;
import marquez.service.models.DatasetVersion;
import org.jdbi.v3.sqlobject.CreateSqlObject;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

@RegisterRowMapper(DatasetVersionMapper.class)
public interface DatasetVersionDao {
  String SELECT =
      "SELECT dv.*, "
          + "ARRAY(SELECT dataset_field_uuid "
          + "      FROM dataset_versions_field_mapping "
          + "      WHERE dataset_version_uuid = dv.uuid) AS field_uuids "
          + "FROM dataset_versions dv ";

  String EXTENDED_SELECT =
      "SELECT dv.*, d.name as dataset_name, n.name as namespace_name, n.uuid as namespace_uuid, "
          + "ARRAY(SELECT dataset_field_uuid "
          + "      FROM dataset_versions_field_mapping "
          + "      WHERE dataset_version_uuid = dv.uuid) AS field_uuids "
          + "FROM dataset_versions AS dv "
          + "INNER JOIN datasets AS d ON d.uuid = dv.dataset_uuid "
          + "INNER JOIN namespaces AS n ON n.uuid = d.namespace_uuid ";

  @SqlQuery(SELECT + "WHERE uuid = :uuid")
  Optional<DatasetVersion> findBy(UUID uuid);

  @SqlQuery(SELECT + "WHERE version = :version)")
  Optional<DatasetVersion> findByVersion(UUID version);

//
//  default Optional<DatasetVersion> find(String typeString, @Nullable UUID uuid) {
//    if (uuid == null) {
//      return Optional.empty();
//    }
//
//    final DatasetType type = DatasetType.valueOf(typeString);
//    switch (type) {
//      case STREAM:
//        return createStreamVersionDao().findBy(uuid).map(DatasetVersion.class::cast);
//      default:
//        return findBy(uuid);
//    }
//  }

  @SqlQuery("SELECT COUNT(*) FROM dataset_versions")
  int count();

  /**
   * returns all Dataset Versions created by this run id
   *
   * @param runId - the run ID
   */
  @SqlQuery(EXTENDED_SELECT + " WHERE run_uuid = :runId")
  @RegisterRowMapper(DatasetVersionMapper.class)
  List<DatasetVersion> findByRunId(@NonNull UUID runId);
}
