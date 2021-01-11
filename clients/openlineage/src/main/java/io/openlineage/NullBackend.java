package io.openlineage;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** A backend that does not do anything. */
class NullBackend implements Backend {

  @Override
  public void put(String path, Object obj) {}

  @Override
  public void post(String path, Object obj) {}

  @Override
  public CompletableFuture postAsync(String path, Object obj) {
    return null;
  }

  @Override
  public void close() throws IOException {}
}
