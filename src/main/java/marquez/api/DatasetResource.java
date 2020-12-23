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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import javax.validation.Valid;
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
import marquez.api.exceptions.DatasetNotFoundException;
import marquez.common.models.DatasetName;
import marquez.common.models.NamespaceName;
import marquez.service.ServiceFactory;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.models.Dataset;
import marquez.service.models.DatasetMeta;

@Path("/api/v1/namespaces/{namespace}/datasets")
public class DatasetResource extends AbstractResource {
  public DatasetResource(ServiceFactory serviceFactory) {
    super(serviceFactory);
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @PUT
  @Path("{dataset}")
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  public Response createOrUpdate(
      @PathParam("namespace") NamespaceName namespaceName,
      @PathParam("dataset") DatasetName datasetName,
      @Valid DatasetMeta datasetMeta)
      throws MarquezServiceException {
    throwIfNotExists(namespaceName);
    datasetMeta.getRunId().ifPresent(this::throwIfNotExists);

    final Dataset dataset = serviceFactory.getDatasetService().createOrUpdate(namespaceName, datasetName, datasetMeta);
    return Response.ok(dataset).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Path("{dataset}")
  @Produces(APPLICATION_JSON)
  public Response get(
      @PathParam("namespace") NamespaceName namespaceName,
      @PathParam("dataset") DatasetName datasetName)
      throws MarquezServiceException {
    throwIfNotExists(namespaceName);

    final Dataset dataset =
        serviceFactory.getDatasetService()
            .get(namespaceName, datasetName)
            .orElseThrow(() -> new DatasetNotFoundException(datasetName));
    return Response.ok(dataset).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Produces(APPLICATION_JSON)
  public Response list(
      @PathParam("namespace") NamespaceName namespaceName,
      @QueryParam("limit") @DefaultValue("100") int limit,
      @QueryParam("offset") @DefaultValue("0") int offset)
      throws MarquezServiceException {
    throwIfNotExists(namespaceName);

    final ImmutableList<Dataset> datasets = serviceFactory.getDatasetService().getAll(namespaceName, limit, offset);
    return Response.ok(new Datasets(datasets)).build();
  }

  @Value
  static class Datasets {
    @NonNull
    @JsonProperty("datasets")
    ImmutableList<Dataset> value;
  }
}
