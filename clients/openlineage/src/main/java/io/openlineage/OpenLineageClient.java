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

package io.openlineage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.inject.internal.util.ImmutableList;
import com.google.inject.internal.util.ImmutableSet;
import io.openlineage.models.DatasetMeta;
import io.openlineage.models.LineageEvent;
import io.openlineage.models.Namespace;
import io.openlineage.models.NamespaceMeta;
import io.openlineage.models.RunState;
import io.openlineage.models.SourceMeta;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import io.openlineage.models.Dataset;
import io.openlineage.models.Job;
import io.openlineage.models.JobMeta;
import io.openlineage.models.Run;
import io.openlineage.models.RunMeta;
import io.openlineage.models.Source;
import io.openlineage.models.Tag;

@Slf4j
public class OpenLineageClient {

  public final MarquezUrl url;
  public final MarquezHttp http;

  public OpenLineageClient(final String baseUriString) {
    this(baseUriString, null);
  }

  public OpenLineageClient(final String baseUriString, final String apiKey) {
    this(Utils.toUrl(baseUriString), apiKey);
  }

  public OpenLineageClient(final URL baseUri) {
    this(baseUri, null);
  }

  public OpenLineageClient(final URL baseUri, final String apiKey) {
    this(MarquezUrl.create(baseUri), MarquezHttp.create(OpenLineageClient.Version.get(), apiKey));
  }

  public OpenLineageClient(@NonNull final MarquezUrl url, @NonNull final MarquezHttp http) {
    this.url = url;
    this.http = http;
  }


  public void emit(LineageEvent event) {
    http.post(url.toLineageUri(), event, LineageEvent.class);
  }

  public static final class Builder {
    public URL baseUrl;
    public String apiKey;

    private Builder() {
      this.baseUrl = DEFAULT_BASE_URL;
    }

    public Builder baseUrl(@NonNull String baseUriString) {
      return baseUrl(Utils.toUrl(baseUriString));
    }

    public Builder baseUrl(@NonNull URL baseUri) {
      this.baseUrl = baseUri;
      return this;
    }

    public Builder apiKey(String apiKey) {
      this.apiKey = apiKey;
      return this;
    }

    public OpenLineageClient build() {
      return new OpenLineageClient(
          MarquezUrl.create(baseUrl), MarquezHttp.create(OpenLineageClient.Version.get(), apiKey));
    }
  }


  @Value
  static class Version {
    private static final String CONFIG_PROPERTIES = "config.properties";

    private static final String VERSION_PROPERTY_NAME = "version";
    private static final String VERSION_UNKNOWN = "unknown";

    @Getter String value;

    private Version(@NonNull final String value) {
      this.value = value;
    }

    static Version get() {
      final Properties properties = new Properties();
      try (final InputStream stream =
          OpenLineageClient.class.getClassLoader().getResourceAsStream(CONFIG_PROPERTIES)) {
        properties.load(stream);
        return new Version(properties.getProperty(VERSION_PROPERTY_NAME, VERSION_UNKNOWN));
      } catch (IOException e) {
        log.warn("Failed to load properties file: {}", CONFIG_PROPERTIES, e);
      }
      return NO_VERSION;
    }

    public static Version NO_VERSION = new Version(VERSION_UNKNOWN);
  }

  @Value
  static class Namespaces {
    @Getter List<Namespace> value;

    @JsonCreator
    Namespaces(@JsonProperty("namespaces") final List<Namespace> value) {
      this.value = ImmutableList.copyOf(value);
    }

    static Namespaces fromJson(final String json) {
      return Utils.fromJson(json, new TypeReference<Namespaces>() {});
    }
  }

  @Value
  static class Sources {
    @Getter List<Source> value;

    @JsonCreator
    Sources(@JsonProperty("sources") final List<Source> value) {
      this.value = ImmutableList.copyOf(value);
    }

    static Sources fromJson(final String json) {
      return Utils.fromJson(json, new TypeReference<Sources>() {});
    }
  }

  @Value
  static class Datasets {
    @Getter List<Dataset> value;

    @JsonCreator
    Datasets(@JsonProperty("datasets") final List<Dataset> value) {
      this.value = ImmutableList.copyOf(value);
    }

    static Datasets fromJson(final String json) {
      return Utils.fromJson(json, new TypeReference<Datasets>() {});
    }
  }

  @Value
  static class Jobs {
    @Getter List<Job> value;

    @JsonCreator
    Jobs(@JsonProperty("jobs") final List<Job> value) {
      this.value = ImmutableList.copyOf(value);
    }

    static Jobs fromJson(final String json) {
      return Utils.fromJson(json, new TypeReference<Jobs>() {});
    }
  }

  @Value
  static class Runs {
    @Getter List<Run> value;

    @JsonCreator
    Runs(@JsonProperty("runs") final List<Run> value) {
      this.value = ImmutableList.copyOf(value);
    }

    static Runs fromJson(final String json) {
      return Utils.fromJson(json, new TypeReference<Runs>() {});
    }
  }

  @Value
  static class Tags {
    @Getter Set<Tag> value;

    @JsonCreator
    Tags(@JsonProperty("tags") final Set<Tag> value) {
      this.value = ImmutableSet.copyOf(value);
    }

    static Tags fromJson(final String json) {
      return Utils.fromJson(json, new TypeReference<Tags>() {});
    }
  }
}
