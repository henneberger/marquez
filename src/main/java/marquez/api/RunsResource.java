package marquez.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.util.List;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import lombok.NonNull;
import lombok.Value;
import marquez.common.models.JobName;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.service.ServiceFactory;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.models.Run;
import marquez.service.models.RunMeta;

@Path("/api/v1")
public class RunsResource extends AbstractResource {

  public RunsResource(ServiceFactory serviceFactory) {
    super(serviceFactory);
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @POST
  @Path("/namespaces/{namespace}/jobs/{job}/runs")
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  public Response create(
      @PathParam("namespace") NamespaceName namespaceName,
      @PathParam("job") JobName jobName,
      @Valid RunMeta runMeta,
      @Context UriInfo uriInfo)
      throws MarquezServiceException {
    throwIfNotExists(namespaceName);
    throwIfNotExists(namespaceName, jobName);
    throwIfExists(namespaceName, jobName, runMeta.getId().orElse(null));

    final Run run = serviceFactory.getRunService().createRun(namespaceName, jobName, runMeta);
    final URI runLocation = locationFor(uriInfo, run);
    return Response.created(runLocation).entity(run).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Path("/namespaces/{namespace}/jobs/{job}/runs")
  @Produces(APPLICATION_JSON)
  public Response list(
      @PathParam("namespace") NamespaceName namespaceName,
      @PathParam("job") JobName jobName,
      @QueryParam("limit") @DefaultValue("100") int limit,
      @QueryParam("offset") @DefaultValue("0") int offset)
      throws MarquezServiceException {
    throwIfNotExists(namespaceName);
    throwIfNotExists(namespaceName, jobName);

    final List<Run> runs = serviceFactory.getRunService().getAllRunsFor(namespaceName, jobName, limit, offset);
    return Response.ok(new Runs(runs)).build();
  }

  //Note: Namespace is not included in the path
  @Path("/jobs/runs/{id}")
  public RunResource runResourceRoot(@PathParam("id") RunId runId) {
    throwIfNotExists(runId);
    return new RunResource(runId, serviceFactory);
  }

  @Value
  static class Runs {
    @NonNull
    @JsonProperty("runs")
    List<Run> value;
  }
}
