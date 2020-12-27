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
import marquez.db.mappers.SourceMapper;
import marquez.service.input.SourceUpsertFragment;
import marquez.service.models.Source;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

@RegisterRowMapper(SourceMapper.class)
public interface SourceDao extends SqlObject {

  default Source upsert(SourceUpsertFragment fragment) {
    return withHandle(
        handle -> {
          StringBuilder upsert = new StringBuilder(
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
                  + ":description)"
                  + " ON CONFLICT(name) DO UPDATE SET "
                  + "type = :type"
                  + ", updated_at = :updatedAt"
                  + ", name = :name"
                  + ", connection_url = :connectionUrl ");
          if (fragment.getDescription() != null) {
            upsert.append(", description = :description ");
          }
          upsert.append(" RETURNING uuid, type, created_at, updated_at, name, connection_url, description");
          return handle
              .createQuery(upsert.toString())
              .bind("createdAt", fragment.getCreatedAt())
              .bind("updatedAt", fragment.getUpdatedAt())
              .bind("type", fragment.getType().name())
              .bind("name", fragment.getName())
              .bind("connectionUrl", fragment.getConnectionUrl())
              .bind("description", fragment.getDescription().orElse(null))
              .map(new SourceMapper())
              .one();
        });
  }

  @SqlQuery("SELECT * FROM sources WHERE name = :name")
  Optional<Source> findBy(String name);

  @SqlQuery("SELECT * FROM sources ORDER BY name LIMIT :limit OFFSET :offset")
  List<Source> findAll(int limit, int offset);

  @SqlQuery("SELECT COUNT(*) FROM sources")
  int count();
}
