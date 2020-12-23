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
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import lombok.NonNull;
import lombok.Value;
import marquez.common.models.DatasetName;
import marquez.common.models.FieldName;
import marquez.common.models.NamespaceName;
import marquez.common.models.TagName;
import marquez.service.ServiceFactory;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.models.Dataset;
import marquez.service.models.Tag;

@Path("/api/v1")
public class TagResource extends AbstractResource {

  public TagResource(ServiceFactory serviceFactory) {
    super(serviceFactory);
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Path("tags")
  @Produces(APPLICATION_JSON)
  public Response list(
      @QueryParam("limit") @DefaultValue("100") int limit,
      @QueryParam("offset") @DefaultValue("0") int offset)
      throws MarquezServiceException {
    final List<Tag> tags = serviceFactory.getTagService().list(limit, offset);
    return Response.ok(new Tags(tags)).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @POST
  @Path("/namespaces/{namespace}/datasets/{dataset}/tags/{tag}")
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  public Response tag(
      @PathParam("namespace") NamespaceName namespaceName,
      @PathParam("dataset") DatasetName datasetName,
      @PathParam("tag") TagName tagName)
      throws MarquezServiceException {
    throwIfNotExists(namespaceName);
    throwIfNotExists(namespaceName, datasetName);
    throwIfNotExists(tagName);

    final Dataset dataset = serviceFactory.getDatasetService().tagWith(namespaceName, datasetName, tagName);
    return Response.ok(dataset).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @POST
  @Path("/namespaces/{namespace}/datasets/{dataset}/fields/{field}/tags/{tag}")
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  public Response tagField(
      @PathParam("namespace") NamespaceName namespaceName,
      @PathParam("dataset") DatasetName datasetName,
      @PathParam("field") FieldName fieldName,
      @PathParam("tag") TagName tagName)
      throws MarquezServiceException {
    throwIfNotExists(namespaceName);
    throwIfNotExists(namespaceName, datasetName);
    throwIfNotExists(namespaceName, datasetName, fieldName);
    throwIfNotExists(tagName);

    final Dataset dataset =
        serviceFactory.getDatasetService().tagFieldWith(namespaceName, datasetName, fieldName, tagName);
    return Response.ok(dataset).build();
  }

  @Value
  static class Tags {
    @NonNull
    @JsonProperty("tags")
    List<Tag> value;
  }
}
