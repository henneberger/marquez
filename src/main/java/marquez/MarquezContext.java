package marquez;

import com.github.jasync.sql.db.pool.ConnectionPool;
import com.github.jasync.sql.db.postgresql.PostgreSQLConnection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import marquez.api.DatasetResource;
import marquez.api.JobResource;
import marquez.api.NamespaceResource;
import marquez.api.OpenLineageResource;
import marquez.api.RunsResource;
import marquez.api.SourceResource;
import marquez.api.TagResource;
import marquez.api.exceptions.JdbiExceptionExceptionMapper;
import marquez.api.exceptions.MarquezServiceExceptionMapper;
import marquez.db.DatasetDao;
import marquez.db.DatasetFieldDao;
import marquez.db.DatasetVersionDao;
import marquez.db.JobContextDao;
import marquez.db.JobDao;
import marquez.db.JobVersionDao;
import marquez.db.NamespaceDao;
import marquez.db.NamespaceOwnershipDao;
import marquez.db.OwnerDao;
import marquez.db.RunArgsDao;
import marquez.db.RunDao;
import marquez.db.RunStateDao;
import marquez.db.SourceDao;
import marquez.db.TagDao;
import marquez.service.DatasetService;
import marquez.service.JobService;
import marquez.service.NamespaceService;
import marquez.service.RunService;
import marquez.service.RunTransitionListener;
import marquez.service.ServiceFactory;
import marquez.service.SourceService;
import marquez.service.TagService;
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
  @Getter private final RunsResource runsResource;

  @Getter private final ImmutableList<Object> resources;
  @Getter private final JdbiExceptionExceptionMapper jdbiException;
  private final ServiceFactory serviceFactory;

  private MarquezContext(
      @NonNull final Jdbi jdbi,
      @NonNull final ConnectionPool<PostgreSQLConnection> con,
      @NonNull final ImmutableSet<Tag> tags,
      @NonNull final List<RunTransitionListener> runTransitionListeners)
      throws MarquezServiceException {
    this.serviceFactory = new ServiceFactory(jdbi, con, tags, runTransitionListeners);

    this.serviceExceptionMapper = new MarquezServiceExceptionMapper();
    this.jdbiException = new JdbiExceptionExceptionMapper();

    this.namespaceResource = new NamespaceResource(serviceFactory);
    this.sourceResource = new SourceResource(serviceFactory);
    this.datasetResource =
        new DatasetResource(serviceFactory);
    this.jobResource = new JobResource(serviceFactory);
    this.tagResource = new TagResource(serviceFactory);
    this.runsResource = new RunsResource(serviceFactory);
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
            runsResource);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Jdbi jdbi;
    private ImmutableSet<Tag> tags;
    private List<RunTransitionListener> runTransitionListeners;
    private ConnectionPool<PostgreSQLConnection> con;

    Builder() {
      this.tags = ImmutableSet.of();
      this.runTransitionListeners = new ArrayList<>();
    }

    public Builder jdbi(@NonNull Jdbi jdbi) {
      this.jdbi = jdbi;
      return this;
    }

    public Builder tags(@NonNull ImmutableSet<Tag> tags) {
      this.tags = tags;
      return this;
    }

    public Builder runTransitionListener(@NonNull RunTransitionListener runTransitionListener) {
      return runTransitionListeners(Lists.newArrayList(runTransitionListener));
    }

    public Builder runTransitionListeners(
        @NonNull List<RunTransitionListener> runTransitionListeners) {
      this.runTransitionListeners.addAll(runTransitionListeners);
      return this;
    }

    public MarquezContext build() throws MarquezServiceException {
      return new MarquezContext(jdbi, con, tags, runTransitionListeners);
    }

    public Builder connectionPool(ConnectionPool<PostgreSQLConnection> con) {
      this.con = con;
      return this;
    }
  }
}
