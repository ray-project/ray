package io.ray.api.call;

import io.ray.api.options.ActorCreationOptions;
import java.util.Map;

public class ActorCreatorBase<T extends ActorCreatorBase> {
  private ActorCreationOptions.Builder builder = new ActorCreationOptions.Builder();

  public T setResource(String key, Double value) {
    builder.setResource(key, value);
    return self();
  }

  public T setResources(Map<String, Double> resources) {
    builder.setResources(resources);
    return self();
  }

  public T setMaxRestarts(int maxRestarts) {
    builder.setMaxRestarts(maxRestarts);
    return self();
  }

  public T setJvmOptions(String jvmOptions) {
    builder.setJvmOptions(jvmOptions);
    return self();
  }

  /**
   * See also {@link ActorCreationOptions.Builder#setMaxConcurrency(int)}
   */
  public T setMaxConcurrency(int maxConcurrency) {
    builder.setMaxConcurrency(maxConcurrency);
    return self();
  }

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }

  protected ActorCreationOptions createActorCreationOptions() {
    return builder.createActorCreationOptions();
  }

}
