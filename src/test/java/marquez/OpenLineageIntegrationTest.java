package marquez;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import io.dropwizard.util.Resources;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.Charset;
import marquez.common.models.LineageEvent;
import org.jdbi.v3.testing.JdbiRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTests.class)
public class OpenLineageIntegrationTest {
//  private static final String CONFIG_FILE = "config.test.yml";
//  private static final String CONFIG_FILE_PATH = ResourceHelpers.resourceFilePath(CONFIG_FILE);
//
//  private static final PostgresContainer POSTGRES = PostgresContainer.create("marquez");
//
//  static {
//    POSTGRES.start();
//  }
//
//  @ClassRule public static final JdbiRule dbRule = JdbiRuleInit.init();
//
//  @ClassRule
//  public static final DropwizardAppRule<MarquezConfig> APP =
//      new DropwizardAppRule<>(
//          MarquezApp.class,
//          CONFIG_FILE_PATH,
//          ConfigOverride.config("db.url", POSTGRES.getJdbcUrl()),
//          ConfigOverride.config("db.user", POSTGRES.getUsername()),
//          ConfigOverride.config("db.password", POSTGRES.getPassword()));
//
//  private final String baseUrl = "http://localhost:" + APP.getLocalPort();
//
//  @Test
//  public void testApp_openLineage() throws IOException {
//    URL resource = Resources.getResource("openLineage_events.json");
//    String lineageArr = Resources.toString(resource, Charset.defaultCharset());
//    HttpClient client = HttpClient.newBuilder()
//        .version(Version.HTTP_2)
//        .build();
//
//    HttpRequest request = HttpRequest.newBuilder()
//        .uri(URI.create(baseUrl + "/api/v1/event"))
//        .header("Content-Type", "application/json")
//        .POST(BodyPublishers.ofString(lineageArr))
//        .build();
//
//    client.sendAsync(request, BodyHandlers.ofString())
//        .thenApply(HttpResponse::body)
//        .thenAccept(System.out::println)
//        .join();
//
//  }
//
//  @Test
//  public void test_objectmapper() throws IOException {
//    URL resource = Resources.getResource("openLineage_events.json");
//    String lineageArr = Resources.toString(resource, Charset.defaultCharset());
//    System.out.println("src"+lineageArr);
//    ObjectMapper mapper = new ObjectMapper();
//    mapper.registerModule(new JavaTimeModule());
//    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
//
//    LineageEvent lineageEvent = mapper.readValue(resource, LineageEvent.class);
//    System.out.println(lineageEvent);
//    String outp = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(lineageEvent);
//
//  }
}
