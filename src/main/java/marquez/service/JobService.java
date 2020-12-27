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

package marquez.service;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static marquez.common.Utils.KV_JOINER;
import static marquez.common.Utils.VERSION_JOINER;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.db.JobDao;
import marquez.service.input.JobInsertFragment;
import marquez.service.input.JobServiceFragment;
import marquez.service.models.Job;

@Slf4j
@AllArgsConstructor
public class JobService implements ServiceMetrics {
  private final JobDao jobDao;

  public Job createOrUpdate(@NonNull String namespaceName, @NonNull String jobName, @NonNull JobServiceFragment fragment) {
    ServiceMetrics.emitJobCreationMetric(namespaceName, fragment.getType());
    ServiceMetrics.emitVersionMetric(namespaceName, fragment.getType(), jobName);
    UUID jobVersion = version(namespaceName, jobName, fragment.getLocation(), fragment.getContext());
    JobInsertFragment jobInsertFragment = JobInsertFragment.builder()
        .delegate(fragment)
        .jobVersionUuid(jobVersion)
        .build();
    Job job = jobDao.create(jobInsertFragment);
    log.info("Successfully created job '{}'.", jobName);
    return job;
  }

  public UUID version(@NonNull String namespaceName, @NonNull String jobName, @NonNull String location,
      @NonNull Map<String, String> context) {
    final byte[] bytes =
        VERSION_JOINER
            .join(
                namespaceName,
                jobName,
                location,
                KV_JOINER.join(context))
            .getBytes(UTF_8);
    return UUID.nameUUIDFromBytes(bytes);
  }

  public boolean exists(@NonNull String namespaceName, @NonNull String jobName) {
    return jobDao.exists(namespaceName, jobName);
  }

  public Optional<Job> get(@NonNull String namespaceName, @NonNull String jobName) {
    return jobDao.find(namespaceName, jobName);
  }

  public Optional<Job> getBy(@NonNull String namespace, @NonNull String jobName) {
    return jobDao.find(namespace, jobName);
  }

  public List<Job> getAll(@NonNull String namespaceName, int limit, int offset) {
    checkArgument(limit >= 0, "limit must be >= 0");
    checkArgument(offset >= 0, "offset must be >= 0");
    return jobDao.findAll(namespaceName, limit, offset);
  }
}
