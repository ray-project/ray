package io.ray.serve.replica;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DummyReplica {

  private AtomicInteger counter = new AtomicInteger();

  public String call() {
    return String.valueOf(counter.incrementAndGet());
  }

  public void reconfigure(Object userConfig) {
    counter.set(0);
  }

  public void reconfigure(Map<String, String> userConfig) {
    counter.set(Integer.valueOf(userConfig.get("value")));
  }
}
