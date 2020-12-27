package marquez.api;

import java.net.URI;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.ws.rs.core.UriInfo;
import lombok.NonNull;
import marquez.api.exceptions.DatasetNotFoundException;
import marquez.api.exceptions.FieldNotFoundException;
import marquez.api.exceptions.JobNotFoundException;
import marquez.api.exceptions.NamespaceNotFoundException;
import marquez.api.exceptions.RunAlreadyExistsException;
import marquez.api.exceptions.RunNotFoundException;
import marquez.api.exceptions.SourceNotFoundException;
import marquez.api.exceptions.TagNotFoundException;
import marquez.common.models.DatasetName;
import marquez.common.models.FieldName;
import marquez.common.models.JobName;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.common.models.SourceName;
import marquez.common.models.TagName;
import marquez.service.ServiceFactory;
import marquez.service.models.Namespace;
import marquez.service.models.Run;
import marquez.service.models.Source;

public abstract class AbstractResource {
  protected final ServiceFactory serviceFactory;

  public AbstractResource(ServiceFactory serviceFactory) {
    this.serviceFactory = serviceFactory;
  }

  void throwIfNotExists(@NonNull NamespaceName namespaceName) {
    if (!serviceFactory.getNamespaceService().exists(namespaceName.getValue())) {
      throw new NamespaceNotFoundException(namespaceName);
    }
  }

  Optional<Namespace> getNamespace(@NonNull NamespaceName namespaceName) {
    return serviceFactory.getNamespaceService().get(namespaceName.getValue());
  }

  void throwIfNotExists(@NonNull NamespaceName namespaceName, @NonNull DatasetName datasetName) {
    if (!serviceFactory.getDatasetService().exists(namespaceName.getValue(), datasetName.getValue())) {
      throw new DatasetNotFoundException(datasetName);
    }
  }

  void throwIfNotExists(
      @NonNull NamespaceName namespaceName,
      @NonNull DatasetName datasetName,
      @NonNull FieldName fieldName) {
    if (!serviceFactory.getDatasetService().fieldExists(namespaceName.getValue(), datasetName.getValue(), fieldName.getValue())) {
      throw new FieldNotFoundException(datasetName, fieldName);
    }
  }

  void throwIfNotExists(@NonNull TagName tagName) {
    if (!serviceFactory.getTagService().exists(tagName.getValue())) {
      throw new TagNotFoundException(tagName);
    }
  }

  void throwIfNotExists(@NonNull NamespaceName namespaceName, @NonNull JobName jobName) {
    if (!serviceFactory.getJobService().exists(namespaceName.getValue(), jobName.getValue())) {
      throw new JobNotFoundException(jobName);
    }
  }

  void throwIfExists(
      @NonNull NamespaceName namespaceName, @NonNull JobName jobName, @Nullable RunId runId) {
    if (runId != null) {
      if (serviceFactory.getRunService().exists(runId.getValue())) {
        throw new RunAlreadyExistsException(namespaceName, jobName, runId);
      }
    }
  }

  void throwIfNotExists(@NonNull RunId runId) {
    if (!serviceFactory.getRunService().exists(runId.getValue())) {
      throw new RunNotFoundException(runId);
    }
  }

  protected Source getSourceOrThrowIfNotFound(SourceName sourceName) {
    Optional<Source> source = serviceFactory.getSourceService().get(sourceName.getValue());
    return source.orElseThrow(()-> new SourceNotFoundException(sourceName));
  }

  protected Namespace getNamespaceOrThrowIfNotFound(NamespaceName namespaceName) {
    Optional<Namespace> namespace = getNamespace(namespaceName);
    return namespace.orElseThrow(()-> new NamespaceNotFoundException(namespaceName));
  }

  protected Optional<Run> getRunOrThrowIfNotFound(Optional<RunId> runId) {
    if (runId.isPresent()) {
      Optional<Run> run = getRun(runId.get());
      run.orElseThrow(() -> new RunNotFoundException(runId.get()));
      return run;
    } else {
      return Optional.empty();
    }
  }

  protected Optional<Run> getRun(RunId runId) {
    return serviceFactory.getRunService().get(runId.getValue());
  }

  URI locationFor(@NonNull UriInfo uriInfo,
      @NonNull Run run) {
    return uriInfo
        .getBaseUriBuilder()
        .path(JobResource.class)
        .path(RunListingResource.class, "runResourceRoot")
        .build(run.getUuid());
  }
}
