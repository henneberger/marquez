package marquez.spark.agent;

import java.lang.instrument.Instrumentation;
import lombok.extern.slf4j.Slf4j;
import marquez.spark.agent.transformers.ActiveJobTransformer;
import marquez.spark.agent.transformers.PairRDDFunctionsTransformer;
import marquez.spark.agent.transformers.SparkContextTransformer;

@Slf4j
public class MarquezAgent {
  /**
   * Entry point for -javaagent, pre application start
   */
  public static void premain(String agentArgs, Instrumentation inst) {
    log.info("MarquezAgent.premain " + agentArgs);
    SparkListener.init(agentArgs);
    instrument(inst);
    addShutDownHook();
  }

  /**
   * Entry point when attaching after application start
   */
  public static void agentmain(String agentArgs, Instrumentation inst) {
    premain(agentArgs, inst);
  }

  public static void instrument(Instrumentation inst) {
    inst.addTransformer(new ActiveJobTransformer());
    inst.addTransformer(new SparkContextTransformer());
    inst.addTransformer(new PairRDDFunctionsTransformer());
  }

  private static void addShutDownHook() {
    Runtime.getRuntime()
        .addShutdownHook(new Thread(SparkListener::close));
  }
}
