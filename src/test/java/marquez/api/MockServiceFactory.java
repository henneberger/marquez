package marquez.api;

import static org.mockito.Mockito.mock;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import marquez.service.DatasetService;
import marquez.service.JobService;
import marquez.service.NamespaceService;
import marquez.service.RunService;
import marquez.service.ServiceFactory;
import marquez.service.SourceService;
import marquez.service.TagService;

@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class MockServiceFactory extends ServiceFactory {

  @Builder.Default
  private NamespaceService namespaceService = mock(NamespaceService.class);
  @Builder.Default
  private SourceService sourceService = mock(SourceService.class);
  @Builder.Default
  private DatasetService datasetService = mock(DatasetService.class);
  @Builder.Default
  private JobService jobService = mock(JobService.class);
  @Builder.Default
  private TagService tagService = mock(TagService.class);
  @Builder.Default
  private RunService runService = mock(RunService.class);

  @Override
  public RunService getRunService() {
    return runService;
  }

  @Override
  public NamespaceService getNamespaceService() {
    return namespaceService;
  }

  @Override
  public SourceService getSourceService() {
    return sourceService;
  }

  @Override
  public DatasetService getDatasetService() {
    return datasetService;
  }

  @Override
  public JobService getJobService() {
    return jobService;
  }

  @Override
  public TagService getTagService() {
    return tagService;
  }
}
