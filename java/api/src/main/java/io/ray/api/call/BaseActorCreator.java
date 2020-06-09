package io.ray.api.call;

import io.ray.api.options.ActorCreationOptions;
import java.util.HashMap;
import java.util.Map;

public class BaseActorCreator<T extends BaseActorCreator> {
  private Map<String, Double> resources = new HashMap<>();
  private int maxRestarts = 0;
  private String jvmOptions = null;
  private int maxConcurrency = 1;

  public T setResources(Map<String, Double> resources) {
    this.resources = resources;
    return self();
  }

  public T setMaxRestarts(int maxRestarts) {
    this.maxRestarts = maxRestarts;
    return self();
  }

  public T setJvmOptions(String jvmOptions) {
    this.jvmOptions = jvmOptions;
    return self();
  }

  /**
   * The max concurrency defaults to 1 for threaded execution.
   * Note that the execution order is not guaranteed when max_concurrency > 1.
   *
   * @param maxConcurrency The max number of concurrent calls to allow for this actor.
   * @return self
   */
  public T setMaxConcurrency(int maxConcurrency) {
    if (maxConcurrency <= 0) {
      throw new IllegalArgumentException("maxConcurrency must be greater than 0.");
    }
    this.maxConcurrency = maxConcurrency;
    return self();
  }

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }

  protected ActorCreationOptions createActorCreationOptions() {
    return new ActorCreationOptions.Builder()
        .setResources(resources)
        .setMaxRestarts(maxRestarts)
        .setJvmOptions(jvmOptions)
        .setMaxConcurrency(maxConcurrency)
        .createActorCreationOptions();
  }
}
