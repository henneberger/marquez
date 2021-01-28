package io.openlineage;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * The backend contract for sending Marquez instrumentation. Information operations can be sent
 * synchronously or asynchronously over various protocols
 */
public interface Backend extends Closeable {

  void put(String path, Object obj);

  default void post(String path) {
    post(path, null);
  }

  void post(String path, Object obj);

  CompletableFuture postAsync(String path, Object obj);
}
