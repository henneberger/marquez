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
import marquez.db.mappers.JobVersionMapper;
import marquez.service.models.JobVersion;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

@RegisterRowMapper(JobVersionMapper.class)
public interface JobVersionDao extends SqlObject {
  enum IoType {
    INPUT,
    OUTPUT;
  }

  @SqlQuery("SELECT EXISTS (SELECT 1 FROM job_versions WHERE version = :version)")
  boolean exists(UUID version);

  static final String EXTENDED_SELECT =
      "SELECT j.namespace_uuid, jv.*, jc.uuid AS job_context_uuid, jc.context, n.name as namespace_name, n.uuid as namespace_uuid, j.name, "
          + "ARRAY(SELECT dataset_uuid "
          + "      FROM job_versions_io_mapping "
          + "      WHERE job_version_uuid = jv.uuid AND "
          + "            io_type = 'INPUT') AS input_uuids, "
          + "ARRAY(SELECT dataset_uuid "
          + "      FROM job_versions_io_mapping "
          + "      WHERE job_version_uuid = jv.uuid AND "
          + "            io_type = 'OUTPUT') AS output_uuids "
          + "FROM job_versions AS jv "
          + "INNER JOIN jobs AS j "
          + "  ON j.uuid = jv.job_uuid "
          + "INNER JOIN namespaces AS n "
          + "  ON j.namespace_uuid = n.uuid "
          + "INNER JOIN job_contexts AS jc "
          + "  ON job_context_uuid = jc.uuid ";

  @SqlQuery(EXTENDED_SELECT + "WHERE jv.uuid = :rowUuid")
  Optional<JobVersion> findBy(UUID rowUuid);

  @SqlQuery(
      EXTENDED_SELECT
          + "WHERE n.name = :namespaceName AND j.name = :jobName AND j.current_version_uuid = jv.uuid "
          + "ORDER BY updated_at DESC "
          + "LIMIT 1")
  Optional<JobVersion> findLatest(String namespaceName, String jobName);

  @SqlQuery(EXTENDED_SELECT + "WHERE jv.version = :version")
  Optional<JobVersion> findVersion(UUID version);

  @SqlQuery(
      EXTENDED_SELECT
          + "WHERE n.name = :namespaceName AND j.name = :jobName "
          + "ORDER BY created_at DESC "
          + "LIMIT :limit OFFSET :offset")
  List<JobVersion> findAll(String namespaceName, String jobName, int limit, int offset);

  @SqlQuery("SELECT COUNT(*) FROM job_versions")
  int count();
}
