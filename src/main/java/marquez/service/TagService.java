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

import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.db.TagDao;
import marquez.service.input.TagUpsertFragment;
import marquez.service.models.Tag;

@Slf4j
@AllArgsConstructor
public class TagService implements ServiceMetrics {
  private final TagDao tagDao;
  public void init(@NonNull ImmutableSet<Tag> tags) {
    for (final Tag tag : tags) {
      upsert(new TagUpsertFragment(
          Instant.now(),
          tag.getName(),
          tag.getDescription()));
    }
  }

  public Tag upsert(@NonNull TagUpsertFragment tag) {
    Tag createdTag = tagDao.upsert(tag);
    log.info("Successfully created tag '{}'", createdTag.getName());
    return createdTag;
  }

  public boolean exists(String name) {
    return tagDao.exists(name);
  }

  public List<Tag> list(int limit, int offset) {
    checkArgument(limit >= 0, "limit must be >= 0");
    checkArgument(offset >= 0, "offset must be >= 0");
    return tagDao.findAll(limit, offset);
  }
}
