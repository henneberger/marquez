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
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.NonNull;
import marquez.common.models.Field;
import marquez.db.mappers.DatasetMapper;
import marquez.service.input.DatasetInputFragment;
import marquez.service.input.DatasetInputFragment.FieldFragment;
import marquez.service.input.DatasetServiceFragment.RunFragment;
import marquez.service.models.Dataset;
import marquez.service.models.StreamVersion;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

public interface DatasetDao extends SqlObject {
  @Transaction
  default Dataset upsert(DatasetInputFragment fragment) {
    UUID uuid = withHandle(handle -> {
      UUID datasetUuid = handle
          .createQuery(
              "INSERT INTO datasets ("
                  + "type, "
                  + "created_at, "
                  + "updated_at, "
                  + "namespace_uuid, "
                  + "source_uuid, "
                  + "name, "
                  + "physical_name, "
                  + "description"
                  + ") VALUES ("
                  + ":type, "
                  + ":createdAt, "
                  + ":updatedAt, "
                  + ":namespaceUuid, "
                  + ":sourceUuid, "
                  + ":name, "
                  + ":physicalName, "
                  + ":description) "
                  + "ON CONFLICT (namespace_uuid, name) "
                  //todo: what is the unique identifier here?
                  + "DO UPDATE SET "
                  + "type = :type,"
                  + "updated_at = :updatedAt,"
                  + "source_uuid = :sourceUuid,"
                  + "physical_name = :physicalName,"
                  + "description = :description "
                  + "RETURNING uuid") //todo optional description
          .bind("type", fragment.getType())
          .bind("createdAt", fragment.getNow())
          .bind("updatedAt", fragment.getNow())
          .bind("sourceUuid", fragment.getSource().getUuid())
          .bind("namespaceUuid", fragment.getNamespace().getUuid())
          .bind("name", fragment.getName())
          .bind("physicalName", fragment.getPhysicalName())
          .bind("description", fragment.getDescription())
          .mapTo(UUID.class)
          .one();

      PreparedBatch batch = handle.prepareBatch("INSERT INTO datasets_tag_mapping (dataset_uuid, tag_uuid, tagged_at) "
            + "VALUES (:datasetUuid, :tagUuid, :taggedAt) "
            + "ON CONFLICT DO NOTHING");
      final Instant taggedAt = fragment.getNow();

      for (DatasetInputFragment.TagFragment tagFragment : fragment.getTags()) {
        batch.bind("datasetUuid", datasetUuid)
            .bind("tagUuid", tagFragment.getUuid())
            .bind("taggedAt", taggedAt)
        .add();
      }
      batch.execute();
      UUID datasetVersionUuid;
      if (fragment.getDatasetVersionIdFragment().isPresent()) {
        datasetVersionUuid = fragment.getDatasetVersionIdFragment().get().getUuid();
      } else {
        UUID version = fragment.getVersion();
        // Fields
        datasetVersionUuid = handle.createQuery(
            "INSERT INTO dataset_versions (created_at, dataset_uuid, version, run_uuid) "
                + "VALUES (:createdAt, :datasetUuid, :version, :runUuid) "
                + "ON CONFLICT(version) "
                + "DO UPDATE SET "
                + "run_uuid = :runUuid "
                + "RETURNING uuid")
            .bind("createdAt", fragment.getNow())
            .bind("datasetUuid", datasetUuid)
            .bind("version", version)
            .bind("runUuid", fragment.getRunFragment().map(RunFragment::getUuid).orElse(null))
            .mapTo(UUID.class)
            .one();
      }

      PreparedBatch fieldMappingBatch = handle.prepareBatch(
            "INSERT INTO dataset_versions_field_mapping (dataset_version_uuid, dataset_field_uuid) "
            + "VALUES (:datasetVersionUuid, :datasetFieldUuid) ON CONFLICT DO NOTHING");
      List<FieldFragment> fieldFragments = fragment.getFields();
      for (FieldFragment fieldFragment : fieldFragments) {
        UUID fieldUuid = handle.createQuery(
            "INSERT INTO dataset_fields ("
            + "type, "
            + "created_at, "
            + "updated_at, "
            + "dataset_uuid, "
            + "name, "
            + "description"
            + ") VALUES ("
            + ":type, "
            + ":createdAt, "
            + ":updatedAt, "
            + ":datasetUuid, "
            + ":name, "
            + ":description) "
            + "ON CONFLICT(dataset_uuid, name, type) "
            + "DO UPDATE SET "
            + "updated_at = :updatedAt, "
            + "description = :description "
            + "RETURNING uuid")
            .bind("type", fieldFragment.getType())
            .bind("createdAt", fragment.getNow())
            .bind("updatedAt", fragment.getNow())
            .bind("datasetUuid", datasetUuid)
            .bind("name", fieldFragment.getName())
            .bind("description", fieldFragment.getDescription())
            .mapTo(UUID.class)
            .one();

        fieldMappingBatch
            .bind("datasetVersionUuid", datasetVersionUuid)
            .bind("datasetFieldUuid", fieldUuid)
            .add();
      }
      fieldMappingBatch.execute();

      if (null instanceof StreamVersion) {
        String streamVersion = "INSERT INTO stream_versions (dataset_version_uuid, schema_location) "
            + "VALUES (:uuid, :schemaLocation)";
      }

      handle.createUpdate("UPDATE datasets "
          + "SET current_version_uuid = :currentVersionUuid "
          + "WHERE uuid = :rowUuid")
      .bind("currentVersionUuid", datasetVersionUuid)
      .bind("rowUuid", datasetUuid)
      .execute();

      return datasetUuid;
    });

    return findBy(uuid)
        .get();
  }

  @SqlQuery(
      "SELECT EXISTS ("
          + "SELECT 1 FROM datasets AS d "
          + "INNER JOIN namespaces AS n "
          + "  ON (n.uuid = d.namespace_uuid AND n.name = :namespaceName) "
          + "WHERE d.name = :datasetName)")
  boolean exists(String namespaceName, String datasetName);

  default void updateTags(@NonNull String namespace, @NonNull String datasetName,
      @NonNull String tagName) {
//    final Dataset dataset =
//        datasetDao.find(namespace, datasetName).get();
//    final Tag tagRow =
//        tagDao.findBy(tagName.toUpperCase(Locale.getDefault())).get();
//    final Instant taggedAt = Instant.now();
    //  datasetDao.updateTags(datasetRow.getUuid(), tagRow.getUuid(), taggedAt);
  }
  @SqlUpdate(
      "UPDATE datasets "
          + "SET updated_at = :lastModifiedAt, "
          + "    last_modified_at = :lastModifiedAt "
          + "WHERE uuid IN (<rowUuids>)")
  void updateLastModifiedAt(
      @BindList(onEmpty = NULL_STRING) List<UUID> rowUuids, Instant lastModifiedAt);

  static final String TAG_UUIDS =
      "ARRAY(SELECT tag_uuid "
          + "      FROM datasets_tag_mapping "
          + "      WHERE dataset_uuid = d.uuid) AS tag_uuids ";

  static final String SELECT = "SELECT d.*, " + TAG_UUIDS + "FROM datasets AS d ";

  static final String EXTENDED_SELECT =
      "SELECT d.*, s.name AS source_name, n.name as namespace_name, "
          + TAG_UUIDS
          + "FROM datasets AS d "
          + "INNER JOIN namespaces AS n "
          + "  ON (n.uuid = d.namespace_uuid) "
          + "INNER JOIN sources AS s "
          + "  ON (s.uuid = d.source_uuid) ";

  @SqlQuery(EXTENDED_SELECT + " WHERE d.uuid = :rowUuid")
  @RegisterRowMapper(DatasetMapper.class)
  Optional<Dataset> findBy(UUID rowUuid);

  @SqlQuery(EXTENDED_SELECT + "WHERE d.name = :datasetName AND n.name = :namespaceName")
  @RegisterRowMapper(DatasetMapper.class)
  Optional<Dataset> find(String namespaceName, String datasetName);


  @SqlQuery("SELECT * FROM datasets WHERE name = :datasetName AND n.name = :namespaceName")
  @RegisterRowMapper(DatasetMapper.class)
  Optional<Dataset> findDataset(String namespaceName, String datasetName);


  @SqlQuery(EXTENDED_SELECT + " WHERE d.uuid IN (<rowUuids>)")
  @RegisterRowMapper(DatasetMapper.class)
  List<Dataset> findAllIn(@BindList(onEmpty = NULL_STRING) Collection<UUID> rowUuids);

  @SqlQuery(
      SELECT
          + " INNER JOIN namespaces AS n "
          + "  ON (n.uuid = d.namespace_uuid AND n.name = :namespaceName) "
          + "WHERE d.name IN (<datasetNames>)")
  @RegisterRowMapper(DatasetMapper.class)
  List<Dataset> findAllIn(
      String namespaceName, @BindList(onEmpty = NULL_STRING) Collection<String> datasetNames);

  @SqlQuery(
      EXTENDED_SELECT
          + "WHERE n.name = :namespaceName "
          + "ORDER BY d.name "
          + "LIMIT :limit OFFSET :offset")
  @RegisterRowMapper(DatasetMapper.class)
  List<Dataset> findAll(String namespaceName, int limit, int offset);

  @SqlQuery("SELECT COUNT(*) FROM datasets")
  int count();

//  }
//  if (!exists(namespaceName, datasetName)) {
//    log.info(
//        "No dataset with name '{}' for namespace '{}' found, creating...",
//        datasetName.getValue(),
//        namespaceName.getValue());
//    final Namespace namespace = namespaceDao.findBy(namespaceName.getValue()).get();
//    final Source source = sourceDao.findBy(datasetMeta.getSourceName().getValue()).get();
//    final List<UUID> tagUuids =
//        tagDao
//            .findAllIn(
//                toArray(
//                    datasetMeta.getTags().stream()
//                        .map(TagName::getValue)
//                        .collect(toImmutableList()),
//                    String.class))
//            .stream()
//            .map(Tag::getUuid)
//            .collect(toImmutableList());
//    final DatasetRow newDatasetRow =
//        Mapper.toDatasetRow(
//            namespace.getUuid(), source.getUuid(), datasetName, datasetMeta, tagUuids);
//    datasetDao.insert(newDatasetRow);
//  }
//
//  final Version version = datasetMeta.version(namespaceName, datasetName);
//      if (!versionDao.exists(version.getValue())) {
//    log.info(
//        "Creating version '{}' for dataset '{}'...",
//        version.getValue(),
//        datasetName.getValue());
//    final ExtendedDatasetRow datasetRow =
//        datasetDao.find(namespaceName.getValue(), datasetName.getValue()).get();
//    final List<DatasetFieldRow> fieldRows = fieldDao.findAll(datasetRow.getUuid());
//    final List<DatasetFieldRow> newFieldRows =
//        datasetMeta.getFields().stream()
//            .map(field -> toDatasetFieldRow(datasetRow.getUuid(), field))
//            .collect(toImmutableList());
//    final List<DatasetFieldRow> newFieldRowsForVersion =
//        newFieldRows.stream()
//            .filter(
//                newFieldRow ->
//                    fieldRows.stream()
//                        .noneMatch(
//                            fieldRow ->
//                                newFieldRow.getName().equals(fieldRow.getName())
//                                    && newFieldRow.getType().equals(fieldRow.getType())))
//            .collect(toImmutableList());
//    final List<DatasetFieldRow> fieldRowsForVersion =
//        Stream.concat(
//            fieldRows.stream()
//                .filter(
//                    fieldRow ->
//                        newFieldRows.stream()
//                            .noneMatch(
//                                newFieldRow ->
//                                    newFieldRow.getName().equals(fieldRow.getName())
//                                        && !newFieldRow
//                                        .getType()
//                                        .equals(fieldRow.getType()))),
//            newFieldRowsForVersion.stream())
//            .collect(toImmutableList());
//    final List<UUID> fieldUuids =
//        fieldRowsForVersion.stream().map(DatasetFieldRow::getUuid).collect(toImmutableList());
//    final DatasetVersionRow newVersionRow =
//        Mapper.toDatasetVersionRow(datasetRow.getUuid(), version, fieldUuids, datasetMeta);
//    versionDao.insertWith(newVersionRow, newFieldRowsForVersion);
  //
  /** Creates a {@link DatasetFieldRow} instance from the given {@link Field}. */
//  private DatasetFieldRow toDatasetFieldRow(@NonNull UUID datasetUuid, @NonNull Field field) {
//    final List<UUID> tagUuids =
//        tagDao
//            .findAllIn(
//                toArray(
//                    field.getTags().stream().map(TagName::getValue).collect(toImmutableList()),
//                    String.class))
//            .stream()
//            .map(Tag::getUuid)
//            .collect(toImmutableList());
//    return Mapper.toDatasetFieldRow(datasetUuid, field, tagUuids);
//  }

}
