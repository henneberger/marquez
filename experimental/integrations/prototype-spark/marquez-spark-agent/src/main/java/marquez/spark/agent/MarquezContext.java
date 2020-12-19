package marquez.spark.agent;

import java.io.IOException;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.codehaus.jackson.map.ObjectMapper;

@Slf4j
public class MarquezContext {

  private String jobNamespace;
  private String jobName;
  private String parentRunId;
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
      ObjectMapper mapper = new ObjectMapper();
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

  public void close() {
    try {
      httpclient.close();
//      marquezClient = null;
    } catch (IOException e) {
      e.printStackTrace(System.out);
    }
  }
}
