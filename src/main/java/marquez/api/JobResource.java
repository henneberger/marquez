/*
 *
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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import lombok.NonNull;
import lombok.Value;
import marquez.api.exceptions.JobNotFoundException;
import marquez.common.models.JobName;
import marquez.common.models.NamespaceName;
import marquez.service.ServiceFactory;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.models.Job;
import marquez.service.models.JobMeta;

@Path("/api/v1")
public class JobResource extends AbstractResource {

  public JobResource(ServiceFactory serviceFactory) {
    super(serviceFactory);
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @PUT
  @Path("/namespaces/{namespace}/jobs/{job}")
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  public Response createOrUpdate(
      @PathParam("namespace") NamespaceName namespaceName,
      @PathParam("job") JobName jobName,
      @Valid JobMeta jobMeta)
      throws MarquezServiceException {
    throwIfNotExists(namespaceName);
    jobMeta.getRunId().ifPresent(this::throwIfNotExists);

    final Job job = serviceFactory.getJobService()
        .createOrUpdate(namespaceName, jobName, jobMeta);
    return Response.ok(job).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Path("/namespaces/{namespace}/jobs/{job}")
  @Produces(APPLICATION_JSON)
  public Response get(
      @PathParam("namespace") NamespaceName namespaceName, @PathParam("job") JobName jobName)
      throws MarquezServiceException {
    throwIfNotExists(namespaceName);

    final Job job =
        serviceFactory.getJobService()
            .get(namespaceName, jobName).orElseThrow(() -> new JobNotFoundException(jobName));
    return Response.ok(job).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Path("/namespaces/{namespace}/jobs")
  @Produces(APPLICATION_JSON)
  public Response list(
      @PathParam("namespace") NamespaceName namespaceName,
      @QueryParam("limit") @DefaultValue("100") @Min(value = 0) int limit,
      @QueryParam("offset") @DefaultValue("0") @Min(value = 0) int offset)
      throws MarquezServiceException {
    throwIfNotExists(namespaceName);

    final ImmutableList<Job> jobs = serviceFactory.getJobService()
        .getAll(namespaceName, limit, offset);
    return Response.ok(new Jobs(jobs)).build();
  }

  @Value
  static class Jobs {
    @NonNull
    @JsonProperty("jobs")
    ImmutableList<Job> value;
  }
}
