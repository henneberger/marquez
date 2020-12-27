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
import marquez.db.mappers.JobContextMapper;
import marquez.service.models.JobContext;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

@RegisterRowMapper(JobContextMapper.class)
public interface JobContextDao {
  @SqlQuery("SELECT EXISTS (SELECT 1 FROM job_contexts WHERE checksum = :checksum)")
  boolean exists(String checksum);

  @SqlQuery("SELECT * FROM job_contexts WHERE uuid = :rowUuid")
  Optional<JobContext> findBy(UUID rowUuid);

  @SqlQuery("SELECT * FROM job_contexts WHERE checksum = :checksum")
  Optional<JobContext> findBy(String checksum);

  @SqlQuery("SELECT * FROM job_contexts ORDER BY created_at DESC LIMIT :limit OFFSET :offset")
  List<JobContext> findAll(int limit, int offset);

  @SqlQuery("SELECT COUNT(*) FROM job_contexts")
  int count();
}
