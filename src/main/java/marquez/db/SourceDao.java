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

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import marquez.common.models.SourceName;
import marquez.db.mappers.SourceRowMapper;
import marquez.db.models.SourceRow;
import marquez.service.models.SourceMeta;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

@RegisterRowMapper(SourceRowMapper.class)
public interface SourceDao extends SqlObject {

  default Object upsert(SourceRow sourceRow) {
    return withHandle(
        handle -> {
          String upsert =
              "INSERT INTO sources ("
                  + "type, "
                  + "created_at, "
                  + "updated_at, "
                  + "name, "
                  + "connection_url, "
                  + "description"
                  + ") VALUES ("
                  + ":type, "
                  + ":createdAt, "
                  + ":updatedAt, "
                  + ":name, "
                  + ":connectionUrl, "
                  + ":description) ON CONFLICT(name) DO"
                  + " UPDATE SET "
                  + "type = :type"
                  + ", updated_at = :updatedAt"
                  + ", name = :name"
                  + ", connection_url = :connectionUrl";
          if (sourceRow.getDescription() != null) {
            upsert += ", description = :description";
          }
          upsert += " RETURNING uuid";
          return handle
              .createQuery(upsert)
              .bind("createdAt", Instant.now())
              .bind("updatedAt", Instant.now())
              .bindBean(sourceRow)
              .mapTo(UUID.class)
              .one();
        });
  }

  @SqlQuery("SELECT EXISTS (SELECT 1 FROM sources WHERE name = :name)")
  boolean exists(String name);

  @SqlQuery("SELECT * FROM sources WHERE uuid = :rowUuid")
  Optional<SourceRow> findBy(UUID rowUuid);

  @SqlQuery("SELECT * FROM sources WHERE name = :name")
  Optional<SourceRow> findBy(String name);

  @SqlQuery("SELECT * FROM sources ORDER BY name LIMIT :limit OFFSET :offset")
  List<SourceRow> findAll(int limit, int offset);

  @SqlQuery("SELECT COUNT(*) FROM sources")
  int count();

}
