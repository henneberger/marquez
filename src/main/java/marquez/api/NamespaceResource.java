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
import java.time.Instant;
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
import marquez.api.exceptions.NamespaceNotFoundException;
import marquez.common.models.NamespaceName;
import marquez.service.ServiceFactory;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.input.NamespaceServiceFragment;
import marquez.service.models.Namespace;

@Path("/api/v1")
public class NamespaceResource extends AbstractResource {
  public NamespaceResource(ServiceFactory serviceFactory) {
    super(serviceFactory);
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @PUT
  @Path("/namespaces/{namespace}")
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  public Response createOrUpdate(
      @PathParam("namespace") NamespaceName name, @Valid NamespaceMeta meta)
      throws MarquezServiceException {
    Instant now = Instant.now();
    NamespaceServiceFragment fragment = NamespaceServiceFragment.builder()
        .name(name.getValue())
        .updatedAt(now)
        .createdAt(now)
        .description(meta.getDescription())
        .currentOwnerName(Optional.of(meta.getOwnerName().getValue()))
        .build();
    final Namespace namespace = serviceFactory.getNamespaceService().createOrUpdate(fragment);
    return Response.ok(new NamespaceResponse(namespace)).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Path("/namespaces/{namespace}")
  @Produces(APPLICATION_JSON)
  public Response get(@PathParam("namespace") NamespaceName name) throws MarquezServiceException {
    final NamespaceResponse namespace =
        new NamespaceResponse(
            serviceFactory.getNamespaceService().get(name.getValue()).orElseThrow(() -> new NamespaceNotFoundException(name))
        );
    return Response.ok(namespace).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Path("/namespaces")
  @Produces(APPLICATION_JSON)
  public Response list(
      @QueryParam("limit") @DefaultValue("100") int limit,
      @QueryParam("offset") @DefaultValue("0") int offset)
      throws MarquezServiceException {
    final List<NamespaceResponse> namespaces = serviceFactory.getNamespaceService().getAll(limit, offset)
        .stream().map(NamespaceResponse::new)
        .collect(Collectors.toList());
    return Response.ok(new Namespaces(namespaces)).build();
  }

  @Value
  static class Namespaces {
    @NonNull
    @JsonProperty("namespaces")
    List<NamespaceResponse> value;
  }
}
