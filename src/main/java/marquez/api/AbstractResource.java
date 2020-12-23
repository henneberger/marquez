package marquez.api;

import java.net.URI;
import javax.annotation.Nullable;
import javax.ws.rs.core.UriInfo;
import lombok.NonNull;
import marquez.api.exceptions.DatasetNotFoundException;
import marquez.api.exceptions.FieldNotFoundException;
import marquez.api.exceptions.JobNotFoundException;
import marquez.api.exceptions.NamespaceNotFoundException;
import marquez.api.exceptions.RunAlreadyExistsException;
import marquez.api.exceptions.RunNotFoundException;
import marquez.api.exceptions.TagNotFoundException;
import marquez.common.models.DatasetName;
import marquez.common.models.FieldName;
import marquez.common.models.JobName;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.common.models.TagName;
import marquez.service.ServiceFactory;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.models.Run;

public abstract class AbstractResource {
  protected final ServiceFactory serviceFactory;

  public AbstractResource(ServiceFactory serviceFactory) {
    this.serviceFactory = serviceFactory;
  }

  void throwIfNotExists(@NonNull NamespaceName namespaceName) throws MarquezServiceException {
    if (!serviceFactory.getNamespaceService().exists(namespaceName)) {
      throw new NamespaceNotFoundException(namespaceName);
    }
  }

  void throwIfNotExists(@NonNull NamespaceName namespaceName, @NonNull DatasetName datasetName)
      throws MarquezServiceException {
    if (!serviceFactory.getDatasetService().exists(namespaceName, datasetName)) {
      throw new DatasetNotFoundException(datasetName);
    }
  }

  void throwIfNotExists(
      @NonNull NamespaceName namespaceName,
      @NonNull DatasetName datasetName,
      @NonNull FieldName fieldName)
      throws MarquezServiceException {
    if (!serviceFactory.getDatasetService().fieldExists(namespaceName, datasetName, fieldName)) {
      throw new FieldNotFoundException(datasetName, fieldName);
    }
  }

  void throwIfNotExists(@NonNull TagName tagName) throws MarquezServiceException {
    if (!serviceFactory.getTagService().exists(tagName)) {
      throw new TagNotFoundException(tagName);
    }
  }

  void throwIfNotExists(@NonNull NamespaceName namespaceName, @NonNull JobName jobName)
      throws MarquezServiceException {
    if (!serviceFactory.getJobService().exists(namespaceName, jobName)) {
      throw new JobNotFoundException(jobName);
    }
  }

  void throwIfExists(
      @NonNull NamespaceName namespaceName, @NonNull JobName jobName, @Nullable RunId runId)
      throws MarquezServiceException {
    if (runId != null) {
      if (serviceFactory.getRunService().runExists(runId)) {
        throw new RunAlreadyExistsException(namespaceName, jobName, runId);
      }
    }
  }

  void throwIfNotExists(@NonNull RunId runId) throws MarquezServiceException {
    if (!serviceFactory.getRunService().runExists(runId)) {
      throw new RunNotFoundException(runId);
    }
  }

  URI locationFor(@NonNull UriInfo uriInfo,
      @NonNull Run run) {
    return uriInfo
        .getBaseUriBuilder()
        .path(JobResource.class)
        .path(RunsResource.class, "runResourceRoot")
        .build(run.getId().getValue());
  }
}
