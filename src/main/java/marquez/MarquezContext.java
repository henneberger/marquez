package marquez;

import com.github.jasync.sql.db.pool.ConnectionPool;
import com.github.jasync.sql.db.postgresql.PostgreSQLConnection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import marquez.api.DatasetResource;
import marquez.api.JobResource;
import marquez.api.NamespaceResource;
import marquez.api.OpenLineageResource;
import marquez.api.RunListingResource;
import marquez.api.SourceResource;
import marquez.api.TagResource;
import marquez.api.exceptions.JdbiExceptionExceptionMapper;
import marquez.api.exceptions.MarquezServiceExceptionMapper;
import marquez.service.RunTransitionListener;
import marquez.service.ServiceFactory;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.models.Tag;
import org.jdbi.v3.core.Jdbi;

public final class MarquezContext {

  @Getter private final MarquezServiceExceptionMapper serviceExceptionMapper;

  @Getter private final NamespaceResource namespaceResource;
  @Getter private final SourceResource sourceResource;
  @Getter private final DatasetResource datasetResource;
  @Getter private final JobResource jobResource;
  @Getter private final TagResource tagResource;
  @Getter private final OpenLineageResource lineageResource;
  @Getter private final RunListingResource runListingResource;

  @Getter private final ImmutableList<Object> resources;
  @Getter private final JdbiExceptionExceptionMapper jdbiException;
  private final ServiceFactory serviceFactory;

  @Builder
  private MarquezContext(
      @NonNull final Jdbi jdbi,
      @NonNull final ConnectionPool<PostgreSQLConnection> connectionPool,
      @NonNull final ImmutableSet<Tag> tags,
      List<RunTransitionListener> runTransitionListeners)
      throws MarquezServiceException {
    if (runTransitionListeners == null) {
      runTransitionListeners = new ArrayList<>();
    }
    this.serviceFactory = new ServiceFactory(jdbi, connectionPool, tags, runTransitionListeners);

    this.serviceExceptionMapper = new MarquezServiceExceptionMapper();
    this.jdbiException = new JdbiExceptionExceptionMapper();

    this.namespaceResource = new NamespaceResource(serviceFactory);
    this.sourceResource = new SourceResource(serviceFactory);
    this.datasetResource =
        new DatasetResource(serviceFactory);
    this.jobResource = new JobResource(serviceFactory);
    this.tagResource = new TagResource(serviceFactory);
    this.runListingResource = new RunListingResource(serviceFactory);
    this.lineageResource =
        new OpenLineageResource(serviceFactory);

    this.resources =
        ImmutableList.of(
            namespaceResource,
            sourceResource,
            datasetResource,
            jobResource,
            tagResource,
            serviceExceptionMapper,
            jdbiException,
            lineageResource,
            runListingResource);
  }
}
