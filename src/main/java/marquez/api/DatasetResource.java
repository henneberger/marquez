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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
import marquez.service.input.DatasetInputFragment.FieldFragment;
import marquez.service.input.DatasetServiceFragment;
import marquez.service.input.DatasetServiceFragment.NamespaceFragment;
import marquez.service.input.DatasetServiceFragment.RunFragment;
import marquez.service.input.DatasetServiceFragment.SourceFragment;
import marquez.service.input.DatasetServiceFragment.TagFragment;
import marquez.service.models.Dataset;
import marquez.service.models.Namespace;
import marquez.service.models.Run;
import marquez.service.models.Source;

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
    Namespace namespace = getNamespaceOrThrowIfNotFound(namespaceName);
    Optional<Run> run = getRunOrThrowIfNotFound(datasetMeta.getRunId());
    Source source = getSourceOrThrowIfNotFound(datasetMeta.getSourceName());

    DatasetServiceFragment fragment = DatasetServiceFragment.builder()
        .type(datasetMeta.getType().name())
        .name(datasetName.getValue())
        .physicalName(datasetMeta.getPhysicalName().getValue())
        .description(datasetMeta.getDescription())
        .version(datasetMeta.version(namespaceName, datasetName).getValue())
        .source(SourceFragment.builder()
            .uuid(source.getUuid())
            .name(source.getName())
            .build())
        .fields(datasetMeta.getFields().stream().map(f->
            FieldFragment.builder()
              .type(f.getType().name())
              .name(f.getName().getValue())
              .description(f.getDescription())
                .tagFragments(f.getTags().stream().map(t->TagFragment.builder().name(t.getValue()).build()).collect(Collectors.toList()))
              .build()
            ).collect(Collectors.toList()))
        .namespace(NamespaceFragment.builder()
            .name(namespace.getName())
            .uuid(namespace.getUuid())
            .build())
        .runFragment(run.map(value -> RunFragment.builder()
            .uuid(value.getUuid())
            .build()))
        .tagFragments(datasetMeta.getTags().stream().map(t->
                TagFragment.builder().name(t.getValue()).build()
            ).collect(Collectors.toList()))
        .type(datasetMeta.getType().name())
        .build();

    final Dataset dataset = serviceFactory.getDatasetService()
        .createOrUpdate(namespaceName.getValue(), datasetName.getValue(), fragment);
    try {
      new ObjectMapper().writeValueAsString(new DatasetResponse(dataset));
    }catch (Exception e) {
      e.printStackTrace();
      System.out.println();
    }
    return Response.ok(new DatasetResponse(dataset)).build();
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
    Namespace namespace = getNamespaceOrThrowIfNotFound(namespaceName);

    final Dataset dataset =
        serviceFactory.getDatasetService()
            .get(namespace.getName(), datasetName.getValue())
            .orElseThrow(() -> new DatasetNotFoundException(datasetName));
    return Response.ok(new DatasetResponse(dataset)).build();
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
    Namespace namespace = getNamespaceOrThrowIfNotFound(namespaceName);

    final List<DatasetResponse> datasets = serviceFactory.getDatasetService()
        .getAll(namespace.getName(), limit, offset)
        .stream()
        .map(DatasetResponse::new)
        .collect(Collectors.toList());
    return Response.ok(new Datasets(datasets)).build();
  }

  @Value
  static class Datasets {
    @NonNull
    @JsonProperty("datasets")
    List<DatasetResponse> value;
  }
}
