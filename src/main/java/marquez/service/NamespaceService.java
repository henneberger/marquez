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

import static com.google.common.base.Preconditions.checkArgument;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.db.NamespaceDao;
import marquez.service.input.NamespaceServiceFragment;
import marquez.service.models.Namespace;

@Slf4j
public class NamespaceService implements ServiceMetrics {
  private final NamespaceDao namespaceDao;
  public static final String DEFAULT_NAMESPACE = "default";
  public static final String DEFAULT_OWNER = "anonymous";

  public NamespaceService(@NonNull final NamespaceDao namespaceDao){
    this.namespaceDao = namespaceDao;
    init();
  }

  private void init() {
    if (!exists(DEFAULT_NAMESPACE)) {
      Instant now = Instant.now();
      NamespaceServiceFragment fragment = NamespaceServiceFragment.builder()
          .name(DEFAULT_NAMESPACE)
          .createdAt(now)
          .updatedAt(now)
          .currentOwnerName(Optional.of(DEFAULT_OWNER))
          .description(Optional.of("The default global namespace for dataset, job, and run metadata "
              + "not belonging to a user-specified namespace."))
          .build();
      createOrUpdate(fragment);
    }
  }

  public Namespace createOrUpdate(NamespaceServiceFragment fragment) {
    Namespace namespace = namespaceDao.upsert(fragment);
    log.info("Successfully created namespace '{}'  with meta: {}", fragment.getName(), fragment);
    namespaces.inc();

    return namespace;
  }

  public boolean exists(@NonNull String name) {
    return namespaceDao.exists(name);
  }

  public Optional<Namespace> get(@NonNull String name) {
    return namespaceDao.findBy(name);
  }

  public List<Namespace> getAll(int limit, int offset) {
    checkArgument(limit >= 0, "limit must be >= 0");
    checkArgument(offset >= 0, "offset must be >= 0");
    return namespaceDao.findAll(limit, offset);
  }
}
