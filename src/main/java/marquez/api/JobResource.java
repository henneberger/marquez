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
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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
import marquez.common.models.DatasetId;
import marquez.common.models.JobName;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.service.ServiceFactory;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.input.JobServiceFragment;
import marquez.service.input.JobServiceFragment.DatasetFragment;
import marquez.service.models.Job;

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
    //validate inputs and outputs in job meta

    JobServiceFragment fragment = JobServiceFragment.builder()
        .namespace(namespaceName.getValue())
        .jobName(jobName.getValue())
        .type(jobMeta.getType())
        .inputs(toDatasetFragment(jobMeta.getInputs()))
        .outputs(toDatasetFragment(jobMeta.getOutputs()))
        .location(jobMeta.getLocation().isPresent() ? jobMeta.getLocation().get().toString() : null)
        .context(jobMeta.getContext())
        .runId(unboxOptional(jobMeta.getRunId()))
        .description(jobMeta.getDescription())
      .build();
    final Job job = serviceFactory.getJobService()
        .createOrUpdate(namespaceName.getValue(), jobName.getValue(), fragment);
    return Response.ok(job).build();
  }

  public static Set<DatasetFragment> toDatasetFragment(Set<DatasetId> datasets) {
    if (datasets.isEmpty()) {
      return ImmutableSet.of();
    }
    return datasets.stream()
          .map(ds ->
              DatasetFragment.builder()
              .datasetName(ds.getName().getValue())
              .namespace(ds.getNamespace().getValue())
              .build()
          )
        .collect(Collectors.toSet());
  }

  private Optional<UUID> unboxOptional(Optional<RunId> runId) {
    if (runId == null || runId.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(runId.get().getValue());
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
            .get(namespaceName.getValue(), jobName.getValue()).orElseThrow(() -> new JobNotFoundException(jobName));
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

    final List<Job> jobs = serviceFactory.getJobService()
        .getAll(namespaceName.getValue(), limit, offset);
    return Response.ok(new Jobs(jobs)).build();
  }

  @Value
  static class Jobs {
    @NonNull
    @JsonProperty("jobs")
    List<Job> value;
  }
}
