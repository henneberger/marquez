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

package marquez.common.models;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import marquez.Generator;
import marquez.common.Utils;
import marquez.service.models.Tag;

public final class ModelGenerator extends Generator {
  private ModelGenerator() {}

  public static NamespaceName newNamespaceName() {
    return NamespaceName.of("test_namespace" + newId());
  }

  public static OwnerName newOwnerName() {
    return OwnerName.of("test_owner" + newId());
  }

  public static SourceType newSourceType() {
    return SourceType.values()[newIdWithBound(SourceType.values().length - 1)];
  }

  public static SourceName newSourceName() {
    return SourceName.of("test_source" + newId());
  }

  public static URI newConnectionUrl() {
    return newConnectionUrlFor(SourceType.POSTGRESQL);
  }

  public static URI newConnectionUrlFor(SourceType type) {
    String connectionUrlString;
    switch (type) {
      case BIGQUERY:
        // FIXME: when not using the jdbc driver ?
        connectionUrlString =
            "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=MyBigQueryProject;OAuthType=1;"
                + newId();
      case PULSAR:
        connectionUrlString = "pulsar://localhost:6650" + newId();
      case ORACLE:
        connectionUrlString = "jdbc:oracle:thin:@localhost:1521:xe" + newId();
      case MYSQL:
        connectionUrlString = "jdbc:mysql://localhost:3306/test" + newId();
        break;
      case POSTGRESQL:
        connectionUrlString = "jdbc:postgresql://localhost:5432/test" + newId();
        break;
      case REDSHIFT:
        connectionUrlString =
            "jdbc:redshift://we.us-west-2.redshift.amazonaws.com:5439/test" + newId();
        break;
      case SNOWFLAKE:
        connectionUrlString = "jdbc:snowflake://we.snowflakecomputing.com/?db=test" + newId();
        break;
      case KAFKA:
        connectionUrlString = "localhost:9092";
        break;
      default:
        throw new IllegalArgumentException();
    }
    return URI.create(connectionUrlString);
  }

  public static ImmutableSet<DatasetId> newDatasetIds(final int limit) {
    return Stream.generate(ModelGenerator::newDatasetId).limit(limit).collect(toImmutableSet());
  }

  public static DatasetId newDatasetId() {
    return newDatasetIdWith(newNamespaceName());
  }

  public static DatasetId newDatasetIdWith(final NamespaceName namespaceName) {
    return new DatasetId(namespaceName, newDatasetName());
  }

  public static ImmutableSet<DatasetName> newDatasetNames(final int limit) {
    return Stream.generate(ModelGenerator::newDatasetName).limit(limit).collect(toImmutableSet());
  }

  public static DatasetName newDatasetName() {
    return DatasetName.of("test_dataset" + newId());
  }

  public static DatasetType newDatasetType() {
    return DatasetType.values()[newIdWithBound(DatasetType.values().length - 1)];
  }

  public static Field newField() {
    return new Field(newFieldName().getValue(), newFieldType(), newTagNames(2), Optional.of(newDescription()));
  }

  public static FieldName newFieldName() {
    return FieldName.of("test_field" + newId());
  }

  public static String newFieldType() {
    return FieldType.values()[newIdWithBound(FieldType.values().length - 1)].name();
  }

  public static ImmutableList<Field> newFields(final int limit) {
    return Stream.generate(ModelGenerator::newField).limit(limit).collect(toImmutableList());
  }

  public static List<Tag> newTagNames(final int limit) {
    return Stream.generate(ModelGenerator::newTagName).limit(limit).map(t->Tag.builder().name(t.getValue()).build()).collect(
        Collectors.toList());
  }

  public static TagName newTagName() {
    return TagName.of("test_tag" + newId());
  }

  public static JobId newJobId() {
    return newJobIdWith(newNamespaceName());
  }

  public static JobId newJobIdWith(final NamespaceName namespaceName) {
    return new JobId(namespaceName, newJobName());
  }

  public static JobName newJobName() {
    return JobName.of("test_job" + newId());
  }

  public static JobType newJobType() {
    return JobType.values()[newIdWithBound(JobType.values().length - 1)];
  }

  public static URL newLocation() {
    return Utils.toUrl("https://github.com/repo/test/commit/" + newId());
  }

  public static ImmutableMap<String, String> newContext() {
    return ImmutableMap.of(
        "sql", String.format("SELECT * FROM room_bookings WHERE room = '%dH';", newId()));
  }

  public static RunId newRunId() {
    return RunId.of(UUID.randomUUID());
  }

  public static String newDescription() {
    return "test_description" + newId();
  }
}
