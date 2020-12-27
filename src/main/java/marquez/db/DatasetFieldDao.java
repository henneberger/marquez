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

import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import marquez.db.mappers.DatasetFieldMapper;
import marquez.service.models.DatasetField;
import marquez.service.models.Tag;
import org.jdbi.v3.sqlobject.CreateSqlObject;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

@RegisterRowMapper(DatasetFieldMapper.class)
public interface DatasetFieldDao extends SqlObject {
  @CreateSqlObject
  TagDao createTagDao();

  @SqlQuery(
      "SELECT EXISTS ("
          + "SELECT 1 FROM dataset_fields AS df "
          + "INNER JOIN datasets AS d "
          + "  ON (d.uuid = df.dataset_uuid AND d.name = :datasetName) "
          + "INNER JOIN namespaces AS n "
          + "  ON (n.uuid = d.namespace_uuid AND n.name = :namespaceName) "
          + "WHERE df.name = :name)")
  boolean exists(String namespaceName, String datasetName, String name);

  @SqlUpdate(
      "INSERT INTO dataset_fields_tag_mapping (dataset_field_uuid, tag_uuid, tagged_at) "
          + "VALUES (:rowUuid, :tagUuid, :taggedAt)")
  void updateTags(UUID rowUuid, UUID tagUuid, Instant taggedAt);

  @SqlQuery(
      "SELECT *, "
          + "ARRAY(SELECT tag_uuid "
          + "      FROM dataset_fields_tag_mapping "
          + "      WHERE dataset_field_uuid = uuid) AS tag_uuids "
          + "FROM dataset_fields WHERE uuid = :rowUuid")
  Optional<DatasetField> findBy(UUID rowUuid);

  @SqlQuery(
      "SELECT *, "
          + "ARRAY(SELECT tag_uuid "
          + "      FROM dataset_fields_tag_mapping "
          + "      WHERE dataset_field_uuid = uuid) AS tag_uuids "
          + "FROM dataset_fields "
          + "WHERE dataset_uuid = :datasetUuid AND name = :name")
  Optional<DatasetField> find(UUID datasetUuid, String name);

  @SqlQuery(
      "SELECT *, "
          + "ARRAY(SELECT tag_uuid "
          + "      FROM dataset_fields_tag_mapping "
          + "      WHERE dataset_field_uuid = uuid) AS tag_uuids "
          + "FROM dataset_fields WHERE uuid IN (<rowUuids>) "
          + "ORDER BY name")
  List<DatasetField> findAllIn(@BindList(onEmpty = NULL_STRING) UUID... rowUuids);

  default List<DatasetField> findAll(UUID datasetUuid) {
    return withHandle(handle-> {
      List<DatasetField> fields = handle.createQuery("SELECT *, "
          + "ARRAY(SELECT tag_uuid "
          + "      FROM dataset_fields_tag_mapping "
          + "      WHERE dataset_field_uuid = uuid) AS tag_uuids "
          + "FROM dataset_fields "
          + "WHERE dataset_uuid = :datasetUuid "
          + "ORDER BY name")
          .bind("datasetUuid", datasetUuid)
          .map(new DatasetFieldMapper())
          .list();
      if (fields == null) {
        return ImmutableList.of();
      }
      for (DatasetField field : fields) {
        List<Tag> tags = field.getTags();
        for (int i = 0; i < tags.size(); i++) {
          Tag tag = tags.get(i);
          Optional<Tag> optionalTag = createTagDao().findBy(tag.getUuid());
          if (optionalTag.isPresent()) {
            tags.set(i, optionalTag.get());
          }
        }
      }
      return fields;
    });
  }

  @SqlQuery("SELECT COUNT(*) FROM dataset_fields")
  int count();
}
