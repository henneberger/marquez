package marquez.graphql;

import graphql.schema.DataFetcher;
import marquez.db.JobVersionDao.IoType;
import marquez.graphql.Model.*;
import org.jdbi.v3.core.Jdbi;

public class GraphQLDataFetchers {

  private Daos dao;

  public void setSource(Jdbi jdbi) {
    this.dao = jdbi.onDemand(Daos.class);
  }

  public DataFetcher getDatasets() {
    return dataFetchingEnvironment -> {
      return dao.getDatasets(10, 0);
    };
  }

  public DataFetcher getSourcesByDataset() {
    return dataFetchingEnvironment -> {
      Dataset dataset = dataFetchingEnvironment.getSource();

      return dao.getSource(dataset.source.uuid);
    };
  }

  public DataFetcher getNamespaceByDataset() {
    return dataFetchingEnvironment -> {
      Dataset dataset = dataFetchingEnvironment.getSource();

      return dao.getNamespace(dataset.namespace.uuid);
    };
  }

  public DataFetcher getCurrentVersionByDataset() {
    return dataFetchingEnvironment -> {
      Dataset dataset = dataFetchingEnvironment.getSource();

      return dao.getCurrentDatasetVersion(dataset.currentVersion.uuid);
    };
  }

  public DataFetcher getFieldsByDataset() {
    return dataFetchingEnvironment -> {
      Dataset dataset = dataFetchingEnvironment.getSource();

      return dao.getDatasetField(dataset.uuid);
    };
  }

  public DataFetcher getJobVersionAsInputByDataset() {
    return dataFetchingEnvironment -> {
      Dataset dataset = dataFetchingEnvironment.getSource();

      return dao.getJobVersionsByIoMapping(dataset.uuid, IoType.INPUT);
    };
  }

  public DataFetcher getVersionAsOutputByDataset() {
    return dataFetchingEnvironment -> {
      Dataset dataset = dataFetchingEnvironment.getSource();

      return dao.getJobVersionsByIoMapping(dataset.uuid, IoType.OUTPUT);
    };
  }

  public DataFetcher getTagsByDataset() {
    return dataFetchingEnvironment -> {
      Dataset dataset = dataFetchingEnvironment.getSource();

      return dao.getTagsByDatasetTag(dataset.uuid);
    };
  }

  public DataFetcher getVersionsByDataset() {
    return dataFetchingEnvironment -> {
      Dataset dataset = dataFetchingEnvironment.getSource();

      return dao.getDatasetVersionsByDataset(dataset.uuid);
    };
  }

  public DataFetcher getDatasetFieldsByTag() {
    return dataFetchingEnvironment -> {
      Tag Tag = dataFetchingEnvironment.getSource();

      return dao.getDatasetFieldsByTagUuid(Tag.uuid);
    };
  }

  public DataFetcher getDatasetsByTag() {
    return dataFetchingEnvironment -> {
      Tag Tag = dataFetchingEnvironment.getSource();

      return dao.getDatasetsByTagUuid(Tag.uuid);
    };
  }

  public DataFetcher getDatasetsBySource() {
    return dataFetchingEnvironment -> {
      Source Source = dataFetchingEnvironment.getSource();

      return dao.getDatasetsBySource(Source.uuid);
    };
  }

  public DataFetcher getRunByRunStateRecord() {
    return dataFetchingEnvironment -> {
      RunStateRecord RunStateRecord = dataFetchingEnvironment.getSource();

      return dao.getRun(RunStateRecord.run.uuid);
    };
  }

  public DataFetcher getRunsByRunArgs() {
    return dataFetchingEnvironment -> {
      RunArgs RunArgs = dataFetchingEnvironment.getSource();

      return dao.getRunsByRunArgs(RunArgs.uuid);
    };
  }

  public DataFetcher getJobVersionByRun() {
    return dataFetchingEnvironment -> {
      Run Run = dataFetchingEnvironment.getSource();

      return dao.getJobVersion(Run.jobVersion.uuid);
    };
  }

  public DataFetcher getRunStatesByRun() {
    return dataFetchingEnvironment -> {
      Run Run = dataFetchingEnvironment.getSource();

      return dao.getRunStateByRun(Run.uuid);
    };
  }

  public DataFetcher getStartStateByRun() {
    return dataFetchingEnvironment -> {
      Run Run = dataFetchingEnvironment.getSource();

      return dao.getRunStateByUuid(Run.startState.uuid);
    };
  }

  public DataFetcher getEndStateByRun() {
    return dataFetchingEnvironment -> {
      Run Run = dataFetchingEnvironment.getSource();

      return dao.getRunStateByUuid(Run.endState.uuid);
    };
  }

  public DataFetcher getInputsByRun() {
    return dataFetchingEnvironment -> {
      Run Run = dataFetchingEnvironment.getSource();

      return dao.getInputsByRun(Run.uuid);
    };
  }

  public DataFetcher getOutputsByRun() {
    return dataFetchingEnvironment -> {
      Run Run = dataFetchingEnvironment.getSource();

      return dao.getOutputsByRun(Run.uuid);
    };
  }

  public DataFetcher getRunArgsByRun() {
    return dataFetchingEnvironment -> {
      Run Run = dataFetchingEnvironment.getSource();

      return dao.getRunArgs(Run.runArgs.uuid);
    };
  }

  public DataFetcher getNamespacesByOwner() {
    return dataFetchingEnvironment -> {
      Owner Owner = dataFetchingEnvironment.getSource();

      return dao.getNamespacesByOwner(Owner.uuid);
    };
  }

  public DataFetcher getOwnersByNamespace() {
    return dataFetchingEnvironment -> {
      Namespace Namespace = dataFetchingEnvironment.getSource();

      return dao.getOwnersByNamespace(Namespace.uuid);
    };
  }

  public DataFetcher getCurrentOwnerByNamespace() {
    return dataFetchingEnvironment -> {
      Namespace Namespace = dataFetchingEnvironment.getSource();

      return dao.getCurrentOwnerByNamespace(Namespace.currentOwner.name);
    };
  }

  public DataFetcher getJobContextByJobVersion() {
    return dataFetchingEnvironment -> {
      JobVersion JobVersion = dataFetchingEnvironment.getSource();

      return dao.getJobContext(JobVersion.jobContext.uuid);
    };
  }

  public DataFetcher getLatestRunByJobVersion() {
    return dataFetchingEnvironment -> {
      JobVersion JobVersion = dataFetchingEnvironment.getSource();
      return JobVersion.latestRun.map(run->dao.getRun(run.uuid))
          .orElse(null);
    };
  }

  public DataFetcher getJobByJobVersion() {
    return dataFetchingEnvironment -> {
      JobVersion JobVersion = dataFetchingEnvironment.getSource();

      return dao.getJob(JobVersion.job.uuid);
    };
  }

  public DataFetcher getInputsByJobVersion() {
    return dataFetchingEnvironment -> {
      JobVersion JobVersion = dataFetchingEnvironment.getSource();

      return dao.getIOMappingByJobVersion(JobVersion.uuid, IoType.INPUT);
    };
  }

  public DataFetcher getOutputsByJobVersion() {
    return dataFetchingEnvironment -> {
      JobVersion JobVersion = dataFetchingEnvironment.getSource();

      return dao.getIOMappingByJobVersion(JobVersion.uuid, IoType.OUTPUT);
    };
  }

  public DataFetcher getJobVersionsByJobContext() {
    return dataFetchingEnvironment -> {
      JobContext JobContext = dataFetchingEnvironment.getSource();

      return dao.getJobVersionByJobContext(JobContext.uuid);
    };
  }

  public DataFetcher getVersionsByJob() {
    return dataFetchingEnvironment -> {
      Job Job = dataFetchingEnvironment.getSource();

      return dao.getJobVersionByJob(Job.uuid);
    };
  }

  public DataFetcher getNamespaceByJob() {
    return dataFetchingEnvironment -> {
      Job Job = dataFetchingEnvironment.getSource();

      return dao.getNamespace(Job.namespace.uuid);
    };
  }

  public DataFetcher getCurrentVersionByJob() {
    return dataFetchingEnvironment -> {
      Job Job = dataFetchingEnvironment.getSource();

      return dao.getJobVersion(Job.currentVersion.uuid);
    };
  }

  public DataFetcher getFieldsByDatasetVersion() {
    return dataFetchingEnvironment -> {
      DatasetVersion DatasetVersion = dataFetchingEnvironment.getSource();

      return dao.getFields(DatasetVersion.uuid);
    };
  }

  public DataFetcher getRunByDatasetVersion() {
    return dataFetchingEnvironment -> {
      DatasetVersion DatasetVersion = dataFetchingEnvironment.getSource();

      return dao.getRun(DatasetVersion.uuid);
    };
  }

  public DataFetcher getDatasetByDatasetField() {
    return dataFetchingEnvironment -> {
      DatasetField DatasetField = dataFetchingEnvironment.getSource();

      return dao.getDataset(DatasetField.uuid);
    };
  }

  public DataFetcher getVersionsByDatasetField() {
    return dataFetchingEnvironment -> {
      DatasetField DatasetField = dataFetchingEnvironment.getSource();

      return dao.getVersionsByDatasetField(DatasetField.uuid);
    };
  }

  public DataFetcher getTagsByDatasetField() {
    return dataFetchingEnvironment -> {
      DatasetField DatasetField = dataFetchingEnvironment.getSource();

      return dao.getTagsByDatasetField(DatasetField.uuid);
    };
  }

  public DataFetcher getDatasetByDatasetVersion() {
    return dataFetchingEnvironment -> {
      DatasetVersion DatasetVersion = dataFetchingEnvironment.getSource();

      return dao.getDataset(DatasetVersion.dataset.uuid);
    };
  }

  public DataFetcher getNamespaceByName() {
    return dataFetchingEnvironment -> {
      String name = dataFetchingEnvironment.getArgument("name");

      return dao.getNamespaceByName(name);
    };
  }

  public DataFetcher getJobsByNamespace() {
    return dataFetchingEnvironment -> {
      Namespace Namespace = dataFetchingEnvironment.getSource();

      return dao.getJobsByNamespace(Namespace.uuid);
    };
  }

  public DataFetcher getDatasetsByNamespace() {
    return dataFetchingEnvironment -> {
      Namespace Namespace = dataFetchingEnvironment.getSource();

      return dao.getDatasetsByNamespace(Namespace.uuid);
    };
  }
}
