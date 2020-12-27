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

package marquez.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import lombok.extern.slf4j.Slf4j;
import marquez.common.models.LineageEvent;
import marquez.service.ServiceFactory;

@Slf4j
@Path("/api/v1/event")
public class OpenLineageResource extends AbstractResource {
  private final Executor executor;

  public OpenLineageResource(ServiceFactory serviceFactory) {
    super(serviceFactory);
    this.executor = Executors.newSingleThreadExecutor();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @POST
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  public void create(LineageEvent event, @Suspended final AsyncResponse asyncResponse) {
    serviceFactory.getLineageDao().write(event, asyncResponse);
//    CompletableFuture.supplyAsync(()->
//        serviceFactory.getLineageDao().marquezModel(event), executor).whenComplete((e, err)->{
//      if (err != null) {
//        log.error("Open lineage marquez update error", err);
//      }
//    });
  }
}
