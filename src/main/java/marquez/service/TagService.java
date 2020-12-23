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
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.models.TagName;
import marquez.db.TagDao;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.models.Tag;

@Slf4j
public class TagService implements ServiceMetrics {
  private final TagDao dao;

  public TagService(@NonNull final TagDao dao) {
    this.dao = dao;
  }

  public void init(@NonNull ImmutableSet<Tag> tags) throws MarquezServiceException {
    for (final Tag tag : tags) {
      upsert(tag);
    }
  }

  public Tag upsert(@NonNull Tag tag) throws MarquezServiceException {
    dao.upsert(TagDao.UpsertTagFragment.build(tag));
    log.info("Successfully created tag '{}'", tag.getName().getValue());
    return tag;
  }

  public boolean exists(@NonNull TagName name) throws MarquezServiceException {
    return dao.exists(name.getValue());
  }

  public List<Tag> list(int limit, int offset) throws MarquezServiceException {
    checkArgument(limit >= 0, "limit must be >= 0");
    checkArgument(offset >= 0, "offset must be >= 0");
    return dao.findAll(limit, offset);
  }
}
