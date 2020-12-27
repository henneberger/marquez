package marquez.api;

import static marquez.api.DatasetResource.Datasets;
import static marquez.common.models.ModelGenerator.newDatasetId;
import static marquez.common.models.ModelGenerator.newFieldName;
import static marquez.common.models.ModelGenerator.newRunId;
import static marquez.common.models.ModelGenerator.newTagName;
import static marquez.service.models.ModelGenerator.newDbTable;
import static marquez.service.models.ModelGenerator.newDbTableMeta;
import static marquez.service.models.ModelGenerator.newDbTableMetaWith;
import static marquez.service.models.ModelGenerator.newDbTableWith;
import static marquez.service.models.ModelGenerator.newNamespaceWith;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.util.Optional;
import javax.ws.rs.core.Response;
import marquez.UnitTests;
import marquez.api.exceptions.DatasetNotFoundException;
import marquez.api.exceptions.FieldNotFoundException;
import marquez.api.exceptions.NamespaceNotFoundException;
import marquez.api.exceptions.RunNotFoundException;
import marquez.api.exceptions.TagNotFoundException;
import marquez.common.models.DatasetId;
import marquez.common.models.DatasetName;
import marquez.common.models.FieldName;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.common.models.TagName;
import marquez.service.DatasetService;
import marquez.service.NamespaceService;
import marquez.service.RunService;
import marquez.service.TagService;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.models.Dataset;
import marquez.service.models.DatasetField;
import marquez.service.models.DbTable;
import marquez.service.models.Run;
import marquez.service.models.Tag;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@Category(UnitTests.class)
public class DatasetResourceTest {
  private static final DatasetId DB_TABLE_ID = newDatasetId();
  private static final NamespaceName NAMESPACE_NAME = DB_TABLE_ID.getNamespace();
  private static final DatasetName DB_TABLE_NAME = DB_TABLE_ID.getName();
  private static final FieldName DB_FIELD_NAME = newFieldName();

  private static final DbTable DB_TABLE_0 = newDbTable();
  private static final DbTable DB_TABLE_1 = newDbTable();
  private static final DbTable DB_TABLE_2 = newDbTable();
  private static final ImmutableList<Dataset> DATASETS =
      ImmutableList.of(DB_TABLE_0, DB_TABLE_1, DB_TABLE_2);

  private static final TagName TAG_NAME = newTagName();
  private static final RunId RUN_ID = newRunId();

  @Rule public MockitoRule rule = MockitoJUnit.rule();

  private NamespaceService namespaceService;
  private DatasetService datasetService;
  private TagService tagService;
  private RunService runService;
  private DatasetResource datasetResource;
  private TagResource tagResource;

  @Before
  public void setUp() {
    MockServiceFactory mockServiceFactory = MockServiceFactory.builder().build();
    datasetResource =
        spy(new DatasetResource(mockServiceFactory));
    tagResource =
        spy(new TagResource(mockServiceFactory));
    namespaceService = mockServiceFactory.getNamespaceService();
    datasetService = mockServiceFactory.getDatasetService();
    tagService = mockServiceFactory.getTagService();
    runService = mockServiceFactory.getRunService();
  }

  @Test
  public void testCreateOrUpdate() throws MarquezServiceException {
    final DbTableMeta dbTableMeta = newDbTableMeta();
    final DbTable dbTable = toDbTable(DB_TABLE_ID, dbTableMeta);

    validNamespace();
    when(datasetService.createOrUpdate(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue(), any()))
        .thenReturn(dbTable);

    final Response response =
        datasetResource.createOrUpdate(NAMESPACE_NAME, DB_TABLE_NAME, dbTableMeta);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat((Dataset) response.getEntity()).isEqualTo(dbTable);
  }

  @Test
  public void testCreateOrUpdateWithRun() throws MarquezServiceException {
    final DbTableMeta dbTableMeta = newDbTableMetaWith(RUN_ID);
    final DbTable dbTable = toDbTable(DB_TABLE_ID, dbTableMeta);

    validNamespace();
    when(runService.get(RUN_ID.getValue())).thenReturn(Optional.of(Run.builder().uuid(RUN_ID.getValue()).build()));
    when(datasetService.createOrUpdate(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue(), any()))
        .thenReturn(dbTable);

    final Response response =
        datasetResource.createOrUpdate(NAMESPACE_NAME, DB_TABLE_NAME, dbTableMeta);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat((Dataset) response.getEntity()).isEqualTo(dbTable);
  }

  @Test
  public void testCreateOrUpdateWithRun_throwOnRunNotFound() throws MarquezServiceException {
    final RunId runIdDoesNotExist = newRunId();
    final DbTableMeta dbTableMeta = newDbTableMetaWith(runIdDoesNotExist);

    validNamespace();
    when(runService.get(RUN_ID.getValue())).thenReturn(null);

    assertThatExceptionOfType(RunNotFoundException.class)
        .isThrownBy(
            () -> datasetResource.createOrUpdate(NAMESPACE_NAME, DB_TABLE_NAME, dbTableMeta))
        .withMessageContaining(String.format("'%s' not found", runIdDoesNotExist.getValue()));
  }

  @Test
  public void testGet() throws MarquezServiceException {
    final DbTable dbTable = newDbTableWith(DB_TABLE_ID);

    validNamespace();
    when(datasetService.get(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue())).thenReturn(Optional.of(dbTable));

    final Response response = datasetResource.get(NAMESPACE_NAME, DB_TABLE_NAME);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat((Dataset) response.getEntity()).isEqualTo(dbTable);
  }

  @Test
  public void testGet_notFound() throws MarquezServiceException {
    validNamespace();
    when(datasetService.get(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue())).thenReturn(Optional.empty());

    assertThatExceptionOfType(DatasetNotFoundException.class)
        .isThrownBy(() -> datasetResource.get(NAMESPACE_NAME, DB_TABLE_NAME))
        .withMessageContaining(String.format("'%s' not found", DB_TABLE_NAME.getValue()));
  }

  @Test
  public void testList() throws MarquezServiceException {
    validNamespace();
    when(datasetService.getAll(NAMESPACE_NAME.getValue(), 4, 0)).thenReturn(DATASETS);

    final Response response = datasetResource.list(NAMESPACE_NAME, 4, 0);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(((Datasets) response.getEntity()).getValue())
        .containsOnly(DB_TABLE_0, DB_TABLE_1, DB_TABLE_2);
  }

  @Test
  public void testList_empty() throws MarquezServiceException {
    validNamespace();
    when(datasetService.getAll(NAMESPACE_NAME.getValue(), 4, 0)).thenReturn(ImmutableList.of());

    final Response response = datasetResource.list(NAMESPACE_NAME, 4, 0);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(((Datasets) response.getEntity()).getValue()).isEmpty();
  }

  @Test
  public void testTag() throws MarquezServiceException {
    validNamespace();
    when(datasetService.exists(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue())).thenReturn(true);
    when(tagService.exists(TAG_NAME.getValue())).thenReturn(true);

    final DbTable dbTable = tagWith(TAG_NAME, newDbTableWith(DB_TABLE_ID));
    when(datasetService.tagWith(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue(), TAG_NAME.getValue())).thenReturn(dbTable);

    final Response response = tagResource.tag(NAMESPACE_NAME, DB_TABLE_NAME, TAG_NAME);
    assertThat(response.getStatus()).isEqualTo(200);

    final Dataset dataset = (Dataset) response.getEntity();
    assertThat(dataset).isEqualTo(dbTable);
    assertThat(dataset.getTags()).contains(Tag.builder().name(TAG_NAME.getValue()).build());
  }

  @Test
  public void testTag_throwOnNamespaceNotFound() throws MarquezServiceException {
    validNamespace();

    assertThatExceptionOfType(NamespaceNotFoundException.class)
        .isThrownBy(() -> tagResource.tag(NAMESPACE_NAME, DB_TABLE_NAME, TAG_NAME))
        .withMessageContaining(String.format("'%s' not found", NAMESPACE_NAME.getValue()));
  }

  @Test
  public void testTag_throwOnDatasetNotFound() throws MarquezServiceException {
    validNamespace();
    when(datasetService.exists(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue())).thenReturn(false);

    assertThatExceptionOfType(DatasetNotFoundException.class)
        .isThrownBy(() -> tagResource.tag(NAMESPACE_NAME, DB_TABLE_NAME, TAG_NAME))
        .withMessageContaining(String.format("'%s' not found", DB_TABLE_NAME.getValue()));
  }

  @Test
  public void testTag_throwOnTagNotFound() throws MarquezServiceException {
    validNamespace();
    when(datasetService.exists(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue())).thenReturn(true);
    when(tagService.exists(TAG_NAME.getValue())).thenReturn(false);

    assertThatExceptionOfType(TagNotFoundException.class)
        .isThrownBy(() -> tagResource.tag(NAMESPACE_NAME, DB_TABLE_NAME, TAG_NAME))
        .withMessageContaining(String.format("'%s' not found", TAG_NAME.getValue()));
  }

  @Test
  public void testTag_field() throws MarquezServiceException {
    when(namespaceService.get(NAMESPACE_NAME.getValue())).thenReturn(null);
    when(datasetService.exists(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue())).thenReturn(true);
    when(datasetService.fieldExists(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue(), DB_FIELD_NAME.getValue())).thenReturn(true);
    when(tagService.exists(TAG_NAME.getValue())).thenReturn(true);

    final DbTable dbTable = tagAllFieldsWith(TAG_NAME, newDbTableWith(DB_TABLE_ID));
    when(datasetService.tagFieldWith(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue(), DB_FIELD_NAME.getValue(), TAG_NAME.getValue()))
        .thenReturn(dbTable);

    final Response response =
        tagResource.tagField(NAMESPACE_NAME, DB_TABLE_NAME, DB_FIELD_NAME, TAG_NAME);
    assertThat(response.getStatus()).isEqualTo(200);

    final Dataset dataset = (Dataset) response.getEntity();
    assertThat(dataset).isEqualTo(dbTable);
    for (final DatasetField field : dataset.getFields()) {
      assertThat(field.getTags()).contains(Tag.builder().name(TAG_NAME.getValue()).build());
    }
  }

  //todo move to tag resource test
  @Test
  public void testTag_field_throwOnNamespaceNotFound() throws MarquezServiceException {
    when(namespaceService.get(NAMESPACE_NAME.getValue())).thenReturn(null);

    assertThatExceptionOfType(NamespaceNotFoundException.class)
        .isThrownBy(
            () -> tagResource.tagField(NAMESPACE_NAME, DB_TABLE_NAME, DB_FIELD_NAME, TAG_NAME))
        .withMessageContaining(String.format("'%s' not found", NAMESPACE_NAME.getValue()));
  }

  @Test
  public void testTag_field_throwOnDatasetNotFound() throws MarquezServiceException {
    validNamespace();
    when(datasetService.exists(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue())).thenReturn(false);

    assertThatExceptionOfType(DatasetNotFoundException.class)
        .isThrownBy(
            () -> tagResource.tagField(NAMESPACE_NAME, DB_TABLE_NAME, DB_FIELD_NAME, TAG_NAME))
        .withMessageContaining(String.format("'%s' not found", DB_TABLE_NAME.getValue()));
  }

  @Test
  public void testTag_field_throwOnFieldNotFound() throws MarquezServiceException {
    validNamespace();
    when(datasetService.exists(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue())).thenReturn(true);
    when(datasetService.fieldExists(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue(), DB_FIELD_NAME.getValue()))
        .thenReturn(false);

    assertThatExceptionOfType(FieldNotFoundException.class)
        .isThrownBy(
            () -> tagResource.tagField(NAMESPACE_NAME, DB_TABLE_NAME, DB_FIELD_NAME, TAG_NAME))
        .withMessageContaining(String.format("'%s' not found", DB_FIELD_NAME.getValue()));
  }

  @Test
  public void testTag_field_throwOnTagNotFound() throws MarquezServiceException {
    validNamespace();
    when(datasetService.exists(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue())).thenReturn(true);
    when(datasetService.fieldExists(NAMESPACE_NAME.getValue(), DB_TABLE_NAME.getValue(), DB_FIELD_NAME.getValue())).thenReturn(true);
    when(tagService.exists(TAG_NAME.getValue())).thenReturn(false);

    assertThatExceptionOfType(TagNotFoundException.class)
        .isThrownBy(
            () -> tagResource.tagField(NAMESPACE_NAME, DB_TABLE_NAME, DB_FIELD_NAME, TAG_NAME))
        .withMessageContaining(String.format("'%s' not found", TAG_NAME.getValue()));
  }

  private void validNamespace() {
    when(namespaceService.get(NAMESPACE_NAME.getValue())).thenReturn(Optional.of(newNamespaceWith(NAMESPACE_NAME)));
  }

  static DbTable toDbTable(final DatasetId dbTableId, final DbTableMeta dbTableMeta) {
    final Instant now = Instant.now();
    return null;
//    return new DbTable(
//        UUID.randomUUID(),
//        DatasetType.DB_TABLE,
//        dbTableId,
//        dbTableId.getName(),
//        dbTableMeta.getPhysicalName(),
//        now,
//        now,
//        dbTableMeta.getSourceName(),
//        dbTableMeta.getFields(),
//        dbTableMeta.getTags(),
//        null,
//        dbTableMeta.getDescription().orElse(null));
  }

  static DbTable tagWith(final TagName tagName, final DbTable dbTable) {
    return null;
//    final ImmutableSet<TagName> tags =
//        ImmutableSet.<TagName>builder().addAll(dbTable.getTags()).add(tagName).build();
//    return new DbTable(
//        dbTable.getId(),
//        dbTable.getName(),
//        dbTable.getPhysicalName(),
//        dbTable.getCreatedAt(),
//        dbTable.getUpdatedAt(),
//        dbTable.getSourceName(),
//        dbTable.getFields(),
//        tags,
//        dbTable.getLastModifiedAt().orElse(null),
//        dbTable.getDescription().orElse(null));
  }

  static DbTable tagAllFieldsWith(final TagName tagName, final DbTable dbTable) {
//    final ImmutableList.Builder<Field> fields = ImmutableList.builder();
//    for (final Field field : dbTable.getFields()) {
//      final ImmutableSet<TagName> tags =
//          ImmutableSet.<TagName>builder().addAll(field.getTags()).add(tagName).build();
//      fields.add(
//          new Field(field.getName(), field.getType(), tags, field.getDescription().orElse(null)));
//    }
//    return new DbTable(
//        dbTable.getId(),
//        dbTable.getName(),
//        dbTable.getPhysicalName(),
//        dbTable.getCreatedAt(),
//        dbTable.getUpdatedAt(),
//        dbTable.getSourceName(),
//        fields.build(),
//        dbTable.getTags(),
//        dbTable.getLastModifiedAt().orElse(null),
//        dbTable.getDescription().orElse(null));
    return null;
  }
}
