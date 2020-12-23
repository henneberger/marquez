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
import marquez.common.models.SourceName;
import marquez.db.SourceDao;
import marquez.service.models.Source;
import marquez.service.models.SourceMeta;

@Slf4j
public class SourceService implements ServiceMetrics {

  private final SourceDao sourceDao;

  public SourceService(@NonNull final SourceDao sourceDao) {
    this.sourceDao = sourceDao;
  }

  public Source createOrUpdate(@NonNull SourceName name, @NonNull SourceMeta meta) {
    Source source = sourceDao.upsert(SourceDao.InputFragment.build(name, meta));
    log.info("Successfully created source '{}' with meta: {}", name.getValue(), meta);
    sources.inc();
    return source;
  }

  public Optional<Source> get(@NonNull SourceName name) {
    return sourceDao.findBy(name.getValue());
  }

  public List<Source> list(int limit, int offset) {
    checkArgument(limit >= 0, "limit must be >= 0");
    checkArgument(offset >= 0, "offset must be >= 0");
    return sourceDao.findAll(limit, offset);
  }
}
