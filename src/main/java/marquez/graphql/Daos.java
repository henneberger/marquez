package marquez.graphql;

import java.util.List;
import java.util.UUID;
import marquez.db.JobVersionDao.IoType;
import marquez.graphql.Model.Dataset;
import marquez.graphql.Model.DatasetField;
import marquez.graphql.Model.DatasetVersion;
import marquez.graphql.Model.Job;
import marquez.graphql.Model.JobContext;
import marquez.graphql.Model.JobVersion;
import marquez.graphql.Model.Namespace;
import marquez.graphql.Model.Owner;
import marquez.graphql.Model.Run;
import marquez.graphql.Model.RunArgs;
import marquez.graphql.Model.RunStateRecord;
import marquez.graphql.Model.Source;
import marquez.graphql.Model.Tag;
import marquez.mapper.DatasetFieldMapper;
import marquez.mapper.DatasetMapper;
import marquez.mapper.DatasetVersionMapper;
import marquez.mapper.JobContextMapper;
import marquez.mapper.JobMapper;
import marquez.mapper.JobVersionMapper;
import marquez.mapper.NamespaceMapper;
import marquez.mapper.OwnerMapper;
import marquez.mapper.RunArgsMapper;
import marquez.mapper.RunMapper;
import marquez.mapper.RunStateRecordMapper;
import marquez.mapper.SourceMapper;
import marquez.mapper.TagMapper;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

public interface Daos {
  @SqlQuery("SELECT * FROM datasets WHERE uuid = :uuid ORDER BY updated_at")
  @RegisterRowMapper(DatasetMapper.class)
  public Dataset getDataset(UUID uuid);

  @SqlQuery("SELECT * FROM datasets LIMIT :limit OFFSET :offset")
  @RegisterRowMapper(DatasetMapper.class)
  public List<Dataset> getDatasets(int limit, int offset);

  @SqlQuery("SELECT * FROM sources where uuid = :uuid")
  @RegisterRowMapper(SourceMapper.class)
  public Source getSource(UUID uuid);

  @SqlQuery("SELECT * FROM namespaces where uuid = :uuid")
  @RegisterRowMapper(NamespaceMapper.class)
  Namespace getNamespace(UUID uuid);

  @SqlQuery("SELECT * FROM dataset_versions where uuid = :uuid LIMIT 10")
  @RegisterRowMapper(DatasetVersionMapper.class)
  List<DatasetVersion> getCurrentDatasetVersion(UUID uuid);

  @SqlQuery("SELECT * FROM dataset_fields where dataset_uuid = :datasetUuid LIMIT 10")
  @RegisterRowMapper(DatasetFieldMapper.class)
  List<DatasetField> getDatasetField(UUID datasetUuid);

  @SqlQuery("SELECT v.* FROM dataset_fields f "
      + " inner join job_versions v "
      + " on f.job_version_uuid = v.uuid"
      + " where f.dataset_uuid = :datasetUuid AND io_type = :ioType LIMIT 10")
  @RegisterRowMapper(JobVersionMapper.class)
  List<JobVersion> getJobVersionsByIoMapping(UUID datasetUuid, IoType ioType);

  @SqlQuery("SELECT t.* FROM datasets_tag_mapping m "
      + " inner join tags t "
      + " on m.tag_uuid = t.uuid"
      + " where dataset_uuid = :datasetUuid LIMIT 10")
  @RegisterRowMapper(TagMapper.class)
  List<Tag> getTagsByDatasetTag(UUID datasetUuid);

  @SqlQuery("SELECT f.* FROM dataset_fields f inner join dataset_fields_tag_mapping m on m.dataset_field_uuid = f.uuid where m.tag_uuid = :tagUuid LIMIT 10")
  @RegisterRowMapper(DatasetFieldMapper.class)
  List<DatasetField> getDatasetFieldsByTagUuid(UUID tagUuid);

  @SqlQuery("SELECT d.* FROM datasets d inner join datasets_tag_mapping m on m.dataset_uuid = d.uuid where tag_uuid = :uuid LIMIT 10")
  @RegisterRowMapper(DatasetMapper.class)
  List<Dataset> getDatasetsByTagUuid(UUID tagUuid);

  @SqlQuery("SELECT d.* from datasets d where source_uuid = :sourceUuid LIMIT 10")
  @RegisterRowMapper(DatasetMapper.class)
  List<Dataset> getDatasetsBySource(UUID sourceUuid);

  @SqlQuery("SELECT * from run_args where uuid = :runArgsUuid LIMIT 10")
  @RegisterRowMapper(RunMapper.class)
  List<Run> getRunsByRunArgs(UUID runArgsUuid);

  @RegisterRowMapper(RunStateRecordMapper.class)
  @SqlQuery("SELECT * from run_states where uuid = :uuid")
  RunStateRecord getRunStateByUuid(UUID uuid);

  @SqlQuery("SELECT dv.* from dataset_versions dv inner join runs_input_mapping m on m.dataset_version_uuid = dv.uuid where m.run_uuid = :runUuid LIMIT 10")
  @RegisterRowMapper(DatasetVersionMapper.class)
  List<DatasetVersion> getInputsByRun(UUID runUuid);

  @SqlQuery("SELECT dv.* from dataset_versions dv where dv.run_uuid = :runUuid LIMIT 10")
  @RegisterRowMapper(DatasetVersionMapper.class)
  List<DatasetVersion> getOutputsByRun(UUID runUuid);

  @RegisterRowMapper(RunArgsMapper.class)
  @SqlQuery("SELECT * from run_args where uuid = :uuid LIMIT 10")
  RunArgs getRunArgs(UUID uuid);

  @SqlQuery("SELECT n.* from namespaces n inner join on namespace_ownerships no on no.namespace_uuid = n.uuid where owner_uuid = :ownerUuid LIMIT 10")
  @RegisterRowMapper(NamespaceMapper.class)
  List<Namespace> getNamespacesByOwner(UUID ownerUuid);

  @SqlQuery("SELECT * from owners o inner join namespace_ownerships no on o.uuid = no.owner_uuid where namespace_uuid = :namespaceUuid LIMIT 10")
  @RegisterRowMapper(OwnerMapper.class)
  List<Owner> getOwnersByNamespace(UUID namespaceUuid);

  @SqlQuery("SELECT * from owners where name = :ownerName LIMIT 10")
  @RegisterRowMapper(OwnerMapper.class)
  Owner getCurrentOwnerByNamespace(String ownerName);

  @SqlQuery("SELECT * from job_contexts where uuid = :uuid LIMIT 10")
  @RegisterRowMapper(JobContextMapper.class)
  JobContext getJobContext(UUID uuid);

  @SqlQuery("SELECT * from runs where uuid = :uuid LIMIT 10")
  @RegisterRowMapper(RunMapper.class)
  Run getRun(UUID uuid);

  @SqlQuery("SELECT * from jobs where uuid = :uuid LIMIT 10")
  @RegisterRowMapper(JobMapper.class)
  Job getJob(UUID uuid);

  @SqlQuery("SELECT d.* from datasets d inner join job_versions_io_mapping m on m.dataset_uuid = d.uuid where m.job_version_uuid = :jobVersionUuid and io_type = :ioType LIMIT 10")
  @RegisterRowMapper(DatasetMapper.class)
  List<Dataset> getIOMappingByJobVersion(UUID jobVersionUuid, IoType ioType);

  @SqlQuery("SELECT * from job_versions where job_context_uuid = :jobContextUuid LIMIT 10")
  @RegisterRowMapper(JobVersionMapper.class)
  List<JobVersion> getJobVersionByJobContext(UUID jobContextUuid);

  @SqlQuery("SELECT * from job_versions where job_uuid = :jobUuid LIMIT 10")
  @RegisterRowMapper(JobVersionMapper.class)
  List<JobVersion> getJobVersionByJob(UUID jobUuid);

  @SqlQuery("SELECT * from job_versions where uuid = :uuid LIMIT 10")
  @RegisterRowMapper(JobVersionMapper.class)
  JobVersion getJobVersion(UUID uuid);

  @SqlQuery("SELECT * from dataset_fields where dataset_uuid = :datasetVersionUuid LIMIT 10")
  @RegisterRowMapper(DatasetFieldMapper.class)
  List<DatasetField> getFields(UUID datasetVersionUuid);

  @SqlQuery("SELECT dv.* from dataset_versions dv inner join dataset_versions_field_mapping m on dv.uuid = m.dataset_version_uuid where dataset_field_uuid = :datasetFieldUuid LIMIT 10")
  @RegisterRowMapper(DatasetVersionMapper.class)
  List<DatasetVersion> getVersionsByDatasetField(UUID datasetFieldUuid);

  @SqlQuery("SELECT t.* from tags t inner join dataset_fields_tag_mapping m on t.uuid = m.tag_uuid where dataaset_field_uuid = :datasetFieldUuid LIMIT 10")
  @RegisterRowMapper(TagMapper.class)
  List<Tag> getTagsByDatasetField(UUID datasetFieldUuid);

  @RegisterRowMapper(DatasetVersionMapper.class)
  @SqlQuery("SELECT * FROM dataset_versions where dataset_uuid = :datasetUuid LIMIT 10")
  List<DatasetVersion> getDatasetVersionsByDataset(UUID datasetUuid);

  @SqlQuery("SELECT * FROM namespaces where name = :name LIMIT 10")
  @RegisterRowMapper(NamespaceMapper.class)
  Namespace getNamespaceByName(String name);

  @SqlQuery("SELECT * from jobs where namespace_uuid = :namespaceUuid LIMIT 10")
  @RegisterRowMapper(JobMapper.class)
  List<Job> getJobsByNamespace(UUID namespaceUuid);

  @SqlQuery("SELECT * from datasets where namespace_uuid = :namespaceUuid LIMIT 10")
  @RegisterRowMapper(DatasetMapper.class)
  List<Dataset> getDatasetsByNamespace(UUID namespaceUuid);

  @SqlQuery("SELECT * from run_states where run_uuid = :runUuid order by transitioned_at desc LIMIT 10")
  @RegisterRowMapper(RunStateRecordMapper.class)
  List<RunStateRecord> getRunStateByRun(UUID runUuid);
}