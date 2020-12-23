package marquez.service;

import com.github.jasync.sql.db.pool.ConnectionPool;
import com.github.jasync.sql.db.postgresql.PostgreSQLConnection;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import marquez.db.OpenLineageDao;
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
import marquez.service.models.Tag;
import org.jdbi.v3.core.Jdbi;

public class ServiceFactory {
  @Getter private NamespaceDao namespaceDao;
  @Getter private OwnerDao ownerDao;
  @Getter private NamespaceOwnershipDao namespaceOwnershipDao;
  @Getter private SourceDao sourceDao;
  @Getter private DatasetDao datasetDao;
  @Getter private DatasetFieldDao datasetFieldDao;
  @Getter private DatasetVersionDao datasetVersionDao;
  @Getter private JobDao jobDao;
  @Getter private JobVersionDao jobVersionDao;
  @Getter private JobContextDao jobContextDao;
  @Getter private RunDao runDao;
  @Getter private RunArgsDao runArgsDao;
  @Getter private RunStateDao runStateDao;
  @Getter private TagDao tagDao;
  @Getter private OpenLineageDao lineageDao;

  @Getter private NamespaceService namespaceService;
  @Getter private SourceService sourceService;
  @Getter private DatasetService datasetService;
  @Getter private JobService jobService;
  @Getter private TagService tagService;
  @Getter private RunService runService;
  
  protected ServiceFactory() {
    
  }

  public ServiceFactory(@NonNull Jdbi jdbi,
      @NonNull ConnectionPool<PostgreSQLConnection> con,
      @NonNull ImmutableSet<Tag> tags,
      @NonNull List<RunTransitionListener> runTransitionListeners) {
    this.namespaceDao = jdbi.onDemand(NamespaceDao.class);
    this.ownerDao = jdbi.onDemand(OwnerDao.class);
    this.namespaceOwnershipDao = jdbi.onDemand(NamespaceOwnershipDao.class);
    this.sourceDao = jdbi.onDemand(SourceDao.class);
    this.datasetDao = jdbi.onDemand(DatasetDao.class);
    this.datasetFieldDao = jdbi.onDemand(DatasetFieldDao.class);
    this.datasetVersionDao = jdbi.onDemand(DatasetVersionDao.class);
    this.jobDao = jdbi.onDemand(JobDao.class);
    this.jobVersionDao = jdbi.onDemand(JobVersionDao.class);
    this.jobContextDao = jdbi.onDemand(JobContextDao.class);
    this.runDao = jdbi.onDemand(RunDao.class);
    this.runArgsDao = jdbi.onDemand(RunArgsDao.class);
    this.runStateDao = jdbi.onDemand(RunStateDao.class);
    this.tagDao = jdbi.onDemand(TagDao.class);
    this.lineageDao = new OpenLineageDao(con, this, runTransitionListeners);
    this.namespaceService = new NamespaceService(namespaceDao, ownerDao, namespaceOwnershipDao);
    this.sourceService = new SourceService(sourceDao);
    this.datasetService =
        new DatasetService(
            namespaceDao, sourceDao, datasetDao, datasetFieldDao, datasetVersionDao, tagDao);
    this.runService =
        new RunService(
            jobVersionDao,
            datasetDao,
            runArgsDao,
            runDao,
            datasetVersionDao,
            runStateDao,
            runTransitionListeners);

    this.jobService =
        new JobService(
            namespaceDao, datasetDao, jobDao, jobVersionDao, jobContextDao, runDao, runService);
    this.tagService = new TagService(tagDao);
    this.tagService.init(tags);
  }
}
