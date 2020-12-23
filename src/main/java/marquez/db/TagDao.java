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

import static org.jdbi.v3.sqlobject.customizer.BindList.EmptyHandling.NULL_STRING;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import marquez.db.mappers.TagRowMapper;
import marquez.service.models.Tag;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

@RegisterRowMapper(TagRowMapper.class)
public interface TagDao {
  @SqlQuery(
      "INSERT INTO tags (created_at, updated_at, name, description) "
          + "VALUES (:createdAt, :updatedAt, :name, :description) ON CONFLICT(name)"
          + " DO UPDATE SET updated_at = :updatedAt, name = :name, description = :description"
          + " RETURNING uuid, created_at, updated_at, name, description")
  Tag upsert(@BindBean UpsertTagFragment row);

  @SqlQuery("SELECT EXISTS (SELECT 1 FROM tags WHERE name = :name)")
  boolean exists(String name);

  @SqlQuery("SELECT * FROM tags WHERE uuid = :rowUuid")
  Optional<Tag> findBy(UUID rowUuid);

  @SqlQuery("SELECT * FROM tags WHERE name = :name")
  Optional<Tag> findBy(String name);

  @SqlQuery("SELECT * FROM tags WHERE uuid IN (<rowUuids>)")
  List<Tag> findAllIn(@BindList(onEmpty = NULL_STRING) UUID... rowUuids);

  @SqlQuery("SELECT * FROM tags WHERE name IN (<names>)")
  List<Tag> findAllIn(@BindList(onEmpty = NULL_STRING) String... names);

  @SqlQuery("SELECT * FROM tags ORDER BY name LIMIT :limit OFFSET :offset")
  List<Tag> findAll(int limit, int offset);

  @SqlQuery("SELECT COUNT(*) FROM tags")
  int count();

  @AllArgsConstructor
  @Getter
  public class UpsertTagFragment {
    public Instant createdAt;
    public Instant updatedAt;
    public String name;
    public Optional<String> description;

    public static UpsertTagFragment build(Tag tag) {
      return new UpsertTagFragment(tag.getCreatedAt() == null ? Instant.now() : tag.getCreatedAt(),
          tag.getUpdatedAt() == null ? Instant.now() : tag.getUpdatedAt(),
          tag.getName().getValue(),
          tag.getDescription());
    }
  }
}
