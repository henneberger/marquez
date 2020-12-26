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
import lombok.NonNull;
import marquez.common.models.SourceName;
import marquez.db.mappers.SourceMapper;
import marquez.db.models.SourceRow;
import marquez.service.mappers.Mapper;
import marquez.service.models.Source;
import marquez.service.models.SourceMeta;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

@RegisterRowMapper(SourceMapper.class)
public interface SourceDao extends SqlObject {

  default Source upsert(SourceRow sourceRow) {
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
          if (sourceRow.getDescription() != null) {
            upsert.append(", description = :description ");
          }
          upsert.append(" RETURNING uuid, type, created_at, updated_at, name, connection_url, description");
          return handle
              .createQuery(upsert.toString())
              .bind("createdAt", Instant.now())
              .bind("updatedAt", Instant.now())
              .bindBean(sourceRow)
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

  public static class InputFragment {
    public static SourceRow build(
        @NonNull final SourceName name, @NonNull final SourceMeta meta) {
      final Instant now = Mapper.newTimestamp();
      return new SourceRow(
          meta.getType().toString(),
          now,
          now,
          name.getValue(),
          meta.getConnectionUrl().toASCIIString(),
          meta.getDescription().orElse(null));
    }
  }
}
