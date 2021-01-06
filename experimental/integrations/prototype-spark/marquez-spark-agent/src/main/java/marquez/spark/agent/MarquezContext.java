package marquez.spark.agent;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.util.Arrays;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

@Slf4j
public class MarquezContext {

  @Getter private static final ObjectMapper mapper = createMapper();
  @Getter private String jobNamespace;
  @Getter private String jobName;
  @Getter private String parentRunId;

  private final CloseableHttpClient httpclient;

  private String get(String[] elements, String name, int index) {
    boolean check = elements.length > index + 1 && name.equals(elements[index]);
    if (check) {
      return elements[index + 1];
    } else {
      log.warn("missing " + name + " in " + Arrays.toString(elements) + " at " + index);
      return "default";
    }
  }

  public MarquezContext(String argument) {
    log.info("Init MarquezContext: " + argument);
    String[] elements = argument.split("\\/");
    String version = get(elements, "api", 1);
    if (version.equals("v1")) {
      log.info("marquez api v1");
    }
    jobNamespace = get(elements, "namespaces", 3);
    jobName = get(elements, "jobs", 5);
    parentRunId = get(elements, "runs", 7);
    log.info(String.format("/api/%s/namespaces/%s/jobs/%s/runs/%s", version, jobNamespace, jobName, parentRunId));
    httpclient = HttpClients.createDefault();
  }

  public void emit(LineageEvent event) {
    try {
      String json = mapper.writeValueAsString(event);
      log.info("calling lineage: {}", json);

      HttpPost httpPost = new HttpPost("http://localhost:5000/api/v1/event");
      StringEntity entity = new StringEntity(json);
      httpPost.setEntity(entity);
      httpPost.setHeader("Accept", "application/json");
      httpPost.setHeader("Content-type", "application/json");

      try (CloseableHttpResponse response2 = httpclient.execute(httpPost)) {
        System.out.println(response2.getCode() + " " + response2.getReasonPhrase());
        HttpEntity entity2 = response2.getEntity();
        EntityUtils.consume(entity2);
      }
    } catch (Exception e) {
      e.printStackTrace();
      log.error("Could not emit lineage", e);
    }
  }

  public static ObjectMapper createMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.setSerializationInclusion(Include.NON_NULL);
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    mapper.disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
    return mapper;
  }

  public void close() {
    try {
      httpclient.close();
    } catch (IOException e) {
      e.printStackTrace(System.out);
    }
  }
}
