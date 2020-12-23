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
import lombok.AllArgsConstructor;
import lombok.Getter;
import marquez.common.models.NamespaceName;
import marquez.common.models.OwnerName;
import marquez.db.mappers.NamespaceMapper;
import marquez.service.models.Namespace;
import marquez.service.models.NamespaceMeta;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

@RegisterRowMapper(NamespaceMapper.class)
public interface NamespaceDao extends SqlObject {
  default Namespace upsert(UpsertNamespaceFragment fragment) {
    return withHandle(
        handle -> {
          String upsert =
              "INSERT INTO namespaces (created_at, updated_at, name, description) "
                  + "VALUES (:createdAt, :updatedAt, :name, :description) ON CONFLICT(name)"
                  + " DO UPDATE SET name = :name RETURNING uuid, created_at, updated_at, name, "
                  + " description, current_owner_name";
          Namespace namespace = handle
              .createQuery(upsert)
              .bindBean(fragment)
              .map(new NamespaceMapper())
              .one();

          //Rather than handle it as a trigger, get the current name and update it
          if (isChangeOwnership(fragment.getCurrentOwnerName(), namespace.getOwnerName())) {
            if (fragment.getCurrentOwnerName().isPresent()) {
              String ownerUpsert =
                  "INSERT INTO owners (created_at, name) " + "VALUES (:createdAt, :name) "
                      + "ON CONFLICT(name) DO UPDATE SET name = EXCLUDED.name RETURNING uuid";
              UUID ownerUuid = handle
                .createQuery(ownerUpsert)
                .bind("createdAt", Instant.now())
                .bind("name", fragment.getCurrentOwnerName().orElse(null))
                .mapTo(UUID.class)
                .one();

              String ownershipUpsert =
                  "INSERT INTO namespace_ownerships (started_at, namespace_uuid, owner_uuid) "
                      + "VALUES (:startedAt, :namespaceUuid, :ownerUuid) "
                      + "ON CONFLICT (namespace_uuid, owner_uuid) DO NOTHING";

              handle
                  .createUpdate(ownershipUpsert)
                  .bind("startedAt", Instant.now())
                  .bind("namespaceUuid", namespace.getUuid())
                  .bind("ownerUuid", ownerUuid)
                  .execute();
            }

            String update =
                "UPDATE namespaces SET current_owner_name = :currentOwnerName WHERE uuid = :uuid";
            handle
                .createUpdate(update)
                .bind("currentOwnerName", fragment.getCurrentOwnerName())
                .bind("uuid", namespace.getUuid())
                .execute();

            namespace.setOwnerName(OwnerName.of(fragment.getCurrentOwnerName().orElse(null)));
          }

          return namespace;
        });
  }

  default boolean isChangeOwnership(Optional<String> fragment, OwnerName namespace) {
    if (fragment == null) {
      return false;
    }
    if (fragment.isEmpty() && namespace != null) {
      return true;
    }
    if (fragment.isPresent() && (namespace == null || !namespace.getValue().equals(fragment.get()))) {
      return true;
    }
    return false;
  }

  @SqlQuery("SELECT EXISTS (SELECT 1 FROM namespaces WHERE name = :name)")
  boolean exists(String name);

  @SqlUpdate(
      "UPDATE namespaces "
          + "SET updated_at = :updatedAt, "
          + "    current_owner_name = :currentOwnerName "
          + "WHERE uuid = :rowUuid")
  void update(UUID rowUuid, Instant updatedAt, String currentOwnerName);

  @SqlQuery("SELECT * FROM namespaces WHERE uuid = :rowUuid")
  Optional<Namespace> findBy(UUID rowUuid);

  @SqlQuery("SELECT * FROM namespaces WHERE name = :name")
  Optional<Namespace> findBy(String name);

  @SqlQuery("SELECT * FROM namespaces ORDER BY name LIMIT :limit OFFSET :offset")
  List<Namespace> findAll(int limit, int offset);

  @SqlQuery("SELECT COUNT(*) FROM namespaces")
  int count();

  @AllArgsConstructor
  @Getter
  public class UpsertNamespaceFragment {
    public Instant createdAt;
    public Instant updatedAt;
    public String name;
    public Optional<String> description;
    public Optional<String> currentOwnerName;

    public static UpsertNamespaceFragment build(NamespaceName name, NamespaceMeta meta) {
      return new UpsertNamespaceFragment(Instant.now(), Instant.now(),
          name.getValue(),
          meta.getDescription(),
          Optional.ofNullable(meta.getOwnerName().getValue()));
    }

    public static UpsertNamespaceFragment build(String namespace) {
      return new UpsertNamespaceFragment(Instant.now(), Instant.now(),
          namespace, null, null);
    }

    public static UpsertNamespaceFragment build(Namespace namespaceRow) {
      return new UpsertNamespaceFragment(namespaceRow.getCreatedAt(), namespaceRow.getUpdatedAt(),
          namespaceRow.getName().getValue(), namespaceRow.getDescription(),
          Optional.of(namespaceRow.getOwnerName().getValue()));
    }
  }
}
