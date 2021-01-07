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

package marquez;

import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring;

import com.codahale.metrics.jdbi3.InstrumentedSqlLogger;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.smoketurner.dropwizard.graphql.GraphQLBundle;
import com.smoketurner.dropwizard.graphql.GraphQLFactory;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.db.ManagedDataSource;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.UUID;
import javax.sql.DataSource;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import marquez.db.DbMigration;
import marquez.db.FlywayFactory;
import marquez.graphql.GraphQLDataFetchers;
import org.flywaydb.core.api.FlywayException;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.postgres.PostgresPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

@Slf4j
public final class MarquezApp extends Application<MarquezConfig> {
  private static final String APP_NAME = "MarquezApp";
  private static final String DB_SOURCE_NAME = APP_NAME + "-source";
  private static final String DB_POSTGRES = "postgresql";
  private static final boolean ERROR_ON_UNDEFINED = false;

  // Monitoring
  private static final String PROMETHEUS = "prometheus";
  private static final String PROMETHEUS_ENDPOINT = "/metrics";
  GraphQLDataFetchers dataFetchers;

  public static void main(final String[] args) throws Exception {
    new MarquezApp().run(args);
  }

  @Override
  public String getName() {
    return APP_NAME;
  }

  @Override
  public void initialize(@NonNull Bootstrap<MarquezConfig> bootstrap) {
    // Enable metric collection for prometheus.
    CollectorRegistry.defaultRegistry.register(
        new DropwizardExports(bootstrap.getMetricRegistry()));
    DefaultExports.initialize(); // Add metrics for CPU, JVM memory, etc.

    // Enable variable substitution with environment variables.
    bootstrap.setConfigurationSourceProvider(
        new SubstitutingSourceProvider(
            bootstrap.getConfigurationSourceProvider(),
            new EnvironmentVariableSubstitutor(ERROR_ON_UNDEFINED)));

    bootstrap.getObjectMapper().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    final GraphQLBundle<MarquezConfig> bundle = new GraphQLBundle<MarquezConfig>() {
      @Override
      public void initialize(Bootstrap<?> bootstrap) {
        bootstrap.addBundle(new AssetsBundle("/assets", "/gql", "index.htm", "graphql-playground"));
      }

      @SneakyThrows
      @Override
      public GraphQLFactory getGraphQLFactory(MarquezConfig configuration) {

        dataFetchers = new GraphQLDataFetchers();

        final GraphQLFactory factory = new GraphQLFactory();
        // the RuntimeWiring must be configured prior to the run()
        // methods being called so the schema is connected properly.
        URL url = Resources.getResource("schema.graphqls");
        String sdl = Resources.toString(url, Charsets.UTF_8);
        RuntimeWiring wiring = RuntimeWiring.newRuntimeWiring()
            .type(newTypeWiring("Query")
                .dataFetcher("datasets", dataFetchers.getDatasets())
                .dataFetcher("namespace", dataFetchers.getNamespaceByName()))
            .type(newTypeWiring("Dataset")
                .dataFetcher("source", dataFetchers.getSourcesByDataset())
                .dataFetcher("namespace", dataFetchers.getNamespaceByDataset())
                .dataFetcher("currentVersion", dataFetchers.getCurrentVersionByDataset())
                .dataFetcher("fields", dataFetchers.getFieldsByDataset())
                .dataFetcher("jobVersionAsInput", dataFetchers.getJobVersionAsInputByDataset())
                .dataFetcher("jobVersionAsOutput", dataFetchers.getVersionAsOutputByDataset())
                .dataFetcher("tags", dataFetchers.getTagsByDataset())
                .dataFetcher("versions", dataFetchers.getVersionsByDataset())
            )
            .type(newTypeWiring("Tag")
                .dataFetcher("fields", dataFetchers.getDatasetFieldsByTag())
                .dataFetcher("datasets", dataFetchers.getDatasetsByTag())
            )
            .type(newTypeWiring("Source")
                .dataFetcher("datasets", dataFetchers.getDatasetsBySource())
            )
            .type(newTypeWiring("RunStateRecord")
                .dataFetcher("run", dataFetchers.getRunByRunStateRecord())
            )
            .type(newTypeWiring("RunArgs")
                .dataFetcher("run", dataFetchers.getRunsByRunArgs())
            )
            .type(newTypeWiring("Run")
                .dataFetcher("jobVersion", dataFetchers.getJobVersionByRun())
                .dataFetcher("runArgs", dataFetchers.getRunArgsByRun())
                .dataFetcher("states", dataFetchers.getRunStatesByRun())
                .dataFetcher("startState", dataFetchers.getStartStateByRun())
                .dataFetcher("endState", dataFetchers.getEndStateByRun())
                .dataFetcher("inputs", dataFetchers.getInputsByRun())
                .dataFetcher("outputs", dataFetchers.getOutputsByRun())
            )
            .type(newTypeWiring("Owner")
                .dataFetcher("namespaces", dataFetchers.getNamespacesByOwner())
            )
            .type(newTypeWiring("Namespace")
                .dataFetcher("owners", dataFetchers.getOwnersByNamespace())
                .dataFetcher("currentOwner", dataFetchers.getCurrentOwnerByNamespace())
                .dataFetcher("jobs", dataFetchers.getJobsByNamespace())
                .dataFetcher("datasets", dataFetchers.getDatasetsByNamespace())
            )
            .type(newTypeWiring("JobVersion")
                .dataFetcher("jobContext", dataFetchers.getJobContextByJobVersion())
                .dataFetcher("latestRun", dataFetchers.getLatestRunByJobVersion())
                .dataFetcher("job", dataFetchers.getJobByJobVersion())
                .dataFetcher("inputs", dataFetchers.getInputsByJobVersion())
                .dataFetcher("outputs", dataFetchers.getOutputsByJobVersion())
            )
            .type(newTypeWiring("JobContext")
                .dataFetcher("jobVersion", dataFetchers.getJobVersionsByJobContext())
            )
            .type(newTypeWiring("Job")
                .dataFetcher("versions", dataFetchers.getVersionsByJob())
                .dataFetcher("namespace", dataFetchers.getNamespaceByJob())
                .dataFetcher("currentVersion", dataFetchers.getCurrentVersionByJob())
            )
            .type(newTypeWiring("DatasetVersion")
                .dataFetcher("fields", dataFetchers.getFieldsByDatasetVersion())
                .dataFetcher("run", dataFetchers.getRunByDatasetVersion())
                .dataFetcher("dataset", dataFetchers.getDatasetByDatasetVersion())
            )
            .type(newTypeWiring("DatasetField")
                .dataFetcher("dataset", dataFetchers.getDatasetByDatasetField())
                .dataFetcher("versions", dataFetchers.getVersionsByDatasetField())
                .dataFetcher("tags", dataFetchers.getTagsByDatasetField())
            )
            .scalar(GraphQLScalarType.newScalar()
                .name("UUID")
                .coercing(new Coercing<UUID, String>() {

                  @Override
                  public String serialize(Object dataFetcherResult)
                      throws CoercingSerializeException {
                    return dataFetcherResult.toString();
                  }

                  @Override
                  public UUID parseValue(Object input) throws CoercingParseValueException {
                    return UUID.fromString(input.toString());
                  }

                  @Override
                  public UUID parseLiteral(Object input) throws CoercingParseLiteralException {
                    return UUID.fromString(input.toString());
                  }
                })
                .build())
            .scalar(GraphQLScalarType.newScalar()
                .name("DateTime")
                .coercing(new Coercing<ZonedDateTime, String>() {

                  @Override
                  public String serialize(Object dataFetcherResult)
                      throws CoercingSerializeException {
                    return dataFetcherResult.toString();
                  }

                  @Override
                  public ZonedDateTime parseValue(Object input) throws CoercingParseValueException {
                    return ZonedDateTime.parse(input.toString());
                  }

                  @Override
                  public ZonedDateTime parseLiteral(Object input) throws CoercingParseLiteralException {
                    return ZonedDateTime.parse(input.toString());
                  }
                })
                .build())
            .build();
        GraphQLSchema graphQLSchema = buildSchema(sdl, wiring);
        factory.setGraphQLSchema(graphQLSchema);



        factory.setRuntimeWiring(wiring);
        return factory;
      }
    };
    bootstrap.addBundle(bundle);
  }


  private GraphQLSchema buildSchema(String sdl, RuntimeWiring wiring) {
    TypeDefinitionRegistry typeRegistry = new SchemaParser().parse(sdl);
    RuntimeWiring runtimeWiring = wiring;
    SchemaGenerator schemaGenerator = new SchemaGenerator();
    return schemaGenerator.makeExecutableSchema(typeRegistry, runtimeWiring);
  }

  @Override
  public void run(@NonNull MarquezConfig config, @NonNull Environment env) throws MarquezException {
    final DataSourceFactory sourceFactory = config.getDataSourceFactory();
    final DataSource source = sourceFactory.build(env.metrics(), DB_SOURCE_NAME);

    log.info("Running startup actions...");
    if (config.isMigrateOnStartup()) {
      final FlywayFactory flywayFactory = config.getFlywayFactory();
      try {
        DbMigration.migrateDbOrError(flywayFactory, source);
      } catch (FlywayException errorOnDbMigrate) {
        log.info("Stopping app...");
        // Propagate throwable up the stack.
        onFatalError(errorOnDbMigrate); // Signal app termination.
      }
    }
    registerResources(config, env, source);
    registerServlets(env);
  }

  public void registerResources(
      @NonNull MarquezConfig config, @NonNull Environment env, @NonNull DataSource source) {
    final JdbiFactory factory = new JdbiFactory();
    final Jdbi jdbi =
        factory
            .build(env, config.getDataSourceFactory(), (ManagedDataSource) source, DB_POSTGRES)
            .installPlugin(new SqlObjectPlugin())
            .installPlugin(new PostgresPlugin());
    jdbi.setSqlLogger(new InstrumentedSqlLogger(env.metrics()));
    this.dataFetchers.setSource(jdbi);

    final MarquezContext context =
        MarquezContext.builder().jdbi(jdbi).tags(config.getTags()).build();

    log.debug("Registering resources...");
    for (final Object resource : context.getResources()) {
      env.jersey().register(resource);
    }
  }

  private void registerServlets(@NonNull Environment env) {
    log.debug("Registering servlets...");

    // Expose metrics for monitoring.
    env.servlets().addServlet(PROMETHEUS, new MetricsServlet()).addMapping(PROMETHEUS_ENDPOINT);
  }
}
