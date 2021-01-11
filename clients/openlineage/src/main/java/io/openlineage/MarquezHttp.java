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

import static org.apache.hc.core5.http.HttpHeaders.AUTHORIZATION;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ok2c.hc5.json.http.JsonRequestProducers;
import com.ok2c.hc5.json.http.JsonResponseConsumers;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.async.methods.BasicHttpRequests;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.cookie.StandardCookieSpec;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.Message;
import org.apache.hc.core5.http.message.BasicHttpRequest;
import org.apache.hc.core5.http.ssl.TLS;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.Timeout;


@Slf4j
class MarquezHttp implements Closeable {
  private final CloseableHttpAsyncClient http;
  public final String apiKey;
  private final ObjectMapper objectMapper;
  private final JsonFactory jsonFactory;


  MarquezHttp(
      @NonNull final CloseableHttpAsyncClient http, final String apiKey) {
    http.start();
    this.objectMapper = Utils.newObjectMapper();
    this.jsonFactory = this.objectMapper.getFactory();

    this.http = http;
    this.apiKey = apiKey;
  }

  static MarquezHttp create(final OpenLineageClient.Version version) {
    return create(version, null);
  }

  static MarquezHttp create(
      @NonNull final OpenLineageClient.Version version, final String apiKey) {
    final CloseableHttpAsyncClient http = HttpAsyncClients.customHttp2()
        .setTlsStrategy(ClientTlsStrategyBuilder.create()
            .setSslContext(SSLContexts.createSystemDefault())
            .setTlsVersions(TLS.V_1_3, TLS.V_1_2)
            .build())
        .setIOReactorConfig(IOReactorConfig.custom()
            .setSoTimeout(Timeout.ofSeconds(5))
            .build())
        .setDefaultRequestConfig(RequestConfig.custom()
            .setConnectTimeout(Timeout.ofSeconds(5))
            .setResponseTimeout(Timeout.ofSeconds(5))
            .setCookieSpec(StandardCookieSpec.STRICT)
            .build())
        .setUserAgent(getUserAgent(version))
        .build();
    return new MarquezHttp(http,  apiKey);
  }

  <T> T post(URI uri, Class<T> clazz) {
    return post(uri, null, clazz);
  }

  <T> T post(URI uri, Object obj, Class<T> clazz) {
    log.debug("POST {}: {}", uri, obj);
    return executeSync(BasicHttpRequests.post(uri), obj, clazz);
  }

  <T> T put(URI uri, Object obj, Class<T> clazz) {
    log.debug("POST {}: {}", uri, obj);
    return executeSync(BasicHttpRequests.put(uri), obj, clazz);
  }

  <T> T get(URI uri, TypeReference<T> typeReference) {
    log.debug("GET {}", uri);
    return executeSync(BasicHttpRequests.put(uri), null, typeReference);
  }

  <T> T get(URI uri, Class<T> clazz) {
    log.debug("GET {}", uri);
    return executeSync(BasicHttpRequests.put(uri), null, clazz);
  }

  private void throwOnHttpError(Message<HttpResponse, JsonNode> message) throws MarquezHttpException {
    final int code = message.getHead().getCode();
    if (code >= 400 && code < 600) { // non-2xx
      HttpError error = objectMapper.convertValue(message.getBody(), HttpError.class);
      throw new MarquezHttpException(error);
    }
  }

  <T> T executeSync(BasicHttpRequest request, Object obj, TypeReference<T> typeReference) {
    try {
      Future<Message<HttpResponse, JsonNode>> future = execute(request, obj);

      Message<HttpResponse, JsonNode> message = future.get();
      throwOnHttpError(message);
      return objectMapper.convertValue(message.getBody(), typeReference);
    } catch (InterruptedException | ExecutionException e) {
      throw new MarquezHttpException();
    }
  }

  <T> T executeSync(BasicHttpRequest request, Object obj, Class<T> clazz) {
    try {
      Future<Message<HttpResponse, JsonNode>> future = execute(request, obj);

      Message<HttpResponse, JsonNode> message = future.get();
      throwOnHttpError(message);
      return objectMapper.convertValue(message.getBody(), clazz);
    } catch (InterruptedException | ExecutionException e) {
      throw new MarquezHttpException();
    }
  }

  private Future<Message<HttpResponse, JsonNode>> execute(HttpRequest request, Object obj) {
    addAuthToReqIfKeyPresent(request);

    return http.execute(
        JsonRequestProducers.create(request, obj, objectMapper),
        JsonResponseConsumers.create(jsonFactory),
        new FutureCallback<Message<HttpResponse, JsonNode>>() {
          @Override
          public void completed(Message<HttpResponse, JsonNode> result) {
            System.out.println(result);
          }

          @Override
          public void failed(Exception ex) {
            System.out.println("Error executing HTTP request: " + ex.getMessage());
          }

          @Override
          public void cancelled() {
            System.out.println("HTTP request execution cancelled");
          }

        });
  }

  @Override
  public void close() throws IOException {
    http.close(CloseMode.GRACEFUL);
  }

  private void addAuthToReqIfKeyPresent(final HttpRequest request) {
    if (apiKey != null) {
      request.addHeader(AUTHORIZATION, "Bearer " + apiKey);
    }
  }

  public static String getUserAgent(OpenLineageClient.Version version) {
    return "marquez-java" + "/" + version.getValue();
  }

  @Value
  static class HttpError {
    @Getter Integer code;
    @Getter String message;
    @Getter String details;

    @JsonCreator
    HttpError(
        final Integer code,
        final String message,
        final String details) {
      this.code = code;
      this.message = message;
      this.details = details;
    }
  }
}
