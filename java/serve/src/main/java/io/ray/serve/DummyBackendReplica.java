package io.ray.serve;

import java.util.concurrent.atomic.AtomicInteger;

public class DummyBackendReplica {

  private AtomicInteger counter = new AtomicInteger();

  public String call() {
    return String.valueOf(counter.incrementAndGet());
  }
}
