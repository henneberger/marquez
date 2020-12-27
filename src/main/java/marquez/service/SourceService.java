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
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import marquez.db.SourceDao;
import marquez.service.input.SourceUpsertFragment;
import marquez.service.models.Source;

@Slf4j
@AllArgsConstructor
public class SourceService implements ServiceMetrics {
  private final SourceDao sourceDao;

  public Source createOrUpdate(SourceUpsertFragment fragment) {
    Source source = sourceDao.upsert(fragment);
    log.info("Successfully created source '{}' with meta: {}", fragment.getName(), fragment);
    sources.inc();
    return source;
  }

  public Optional<Source> get(String name) {
    return sourceDao.findBy(name);
  }

  public List<Source> list(int limit, int offset) {
    checkArgument(limit >= 0, "limit must be >= 0");
    checkArgument(offset >= 0, "offset must be >= 0");
    return sourceDao.findAll(limit, offset);
  }
}
