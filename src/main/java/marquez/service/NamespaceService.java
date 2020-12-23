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

import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.models.NamespaceName;
import marquez.common.models.OwnerName;
import marquez.db.NamespaceDao;
import marquez.db.NamespaceDao.UpsertNamespaceFragment;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.models.Namespace;
import marquez.service.models.NamespaceMeta;

@Slf4j
public class NamespaceService implements ServiceMetrics {
  private final NamespaceDao namespaceDao;

  public NamespaceService(
      @NonNull final NamespaceDao namespaceDao)
      throws MarquezServiceException {
    this.namespaceDao = namespaceDao;
    init();
  }

  private void init() throws MarquezServiceException {
    if (!exists(NamespaceName.DEFAULT)) {
      final NamespaceMeta meta =
          new NamespaceMeta(
              OwnerName.ANONYMOUS,
              "The default global namespace for dataset, job, and run metadata "
                  + "not belonging to a user-specified namespace.");
      createOrUpdate(NamespaceName.DEFAULT, meta);
    }
  }

  public Namespace createOrUpdate(@NonNull NamespaceName name, @NonNull NamespaceMeta meta)
      throws MarquezServiceException {
    Namespace namespace = namespaceDao.upsert(UpsertNamespaceFragment.build(name, meta));
    log.info("Successfully created namespace '{}'  with meta: {}", name.getValue(), meta);
    namespaces.inc();

    return namespace;
  }

  public boolean exists(@NonNull NamespaceName name) throws MarquezServiceException {
    return namespaceDao.exists(name.getValue());
  }

  public Optional<Namespace> get(@NonNull NamespaceName name) throws MarquezServiceException {
    return namespaceDao.findBy(name.getValue());
  }

  public List<Namespace> getAll(int limit, int offset) throws MarquezServiceException {
    checkArgument(limit >= 0, "limit must be >= 0");
    checkArgument(offset >= 0, "offset must be >= 0");
    return namespaceDao.findAll(limit, offset);
  }
}
