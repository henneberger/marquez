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


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import lombok.NonNull;

public final class Utils {
  private Utils() {}

  private static final ObjectMapper MAPPER = newObjectMapper();

  public static ObjectMapper newObjectMapper() {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    return mapper;
  }

  public static String toJson(@NonNull final Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static <T> T fromJson(@NonNull final String json, @NonNull final TypeReference<T> type) {
    try {
      return MAPPER.readValue(json, type);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static URL toUrl(@NonNull final String urlString) {
    try {
      return new URL(urlString);
    } catch (MalformedURLException e) {
      throw new AssertionError("Malformed URL: " + urlString, e);
    }
  }
  public static URI toUri(@NonNull final String urlString) {
    try {
      return toUrl(urlString).toURI();
    } catch (URISyntaxException e) {
      throw new AssertionError("Malformed URL: " + urlString, e);
    }
  }
  public static URI toUri(@NonNull final URL url) {
    try {
      return url.toURI();
    } catch (URISyntaxException e) {
      throw new AssertionError("Malformed URL: " + url, e);
    }
  }
}
