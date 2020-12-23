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

import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.models.SourceName;
import marquez.common.models.SourceType;
import marquez.db.SourceDao;
import marquez.db.models.SourceRow;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.mappers.Mapper;
import marquez.service.models.Source;
import marquez.service.models.SourceMeta;

@Slf4j
public class SourceService implements ServiceMetrics {

  private final SourceDao sourceDao;

  public SourceService(@NonNull final SourceDao sourceDao) {
    this.sourceDao = sourceDao;
  }

  public Source createOrUpdate(@NonNull SourceName name, @NonNull SourceMeta meta) {
    sourceDao.upsert(Mapper.toSourceRow(name, meta));
    log.info("Successfully created source '{}' with meta: {}", name.getValue(), meta);
    sources.inc();
    return get(name).get();
  }

  public Optional<Source> get(@NonNull SourceName name) {
    return sourceDao.findBy(name.getValue()).map(SourceService::toSource);
  }

  public ImmutableList<Source> list(int limit, int offset) {
    checkArgument(limit >= 0, "limit must be >= 0");
    checkArgument(offset >= 0, "offset must be >= 0");
    final ImmutableList.Builder<Source> sources = ImmutableList.builder();
    final List<SourceRow> rows = sourceDao.findAll(limit, offset);
    for (final SourceRow row : rows) {
      sources.add(toSource(row));
    }
    return sources.build();
  }

  static Source toSource(@NonNull final SourceRow row) {
    return new Source(
        SourceType.valueOf(row.getType()),
        SourceName.of(row.getName()),
        row.getCreatedAt(),
        row.getUpdatedAt(),
        URI.create(row.getConnectionUrl()),
        row.getDescription().orElse(null));
  }
}
