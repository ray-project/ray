package io.ray.api.call;

import io.ray.api.options.ActorCreationOptions;
import java.util.Map;

/**
 * Base helper to create actor.
 *
 * @param <T> The type of the concrete actor creator
 */
public class BaseActorCreator<T extends BaseActorCreator> {
  protected ActorCreationOptions.Builder builder = new ActorCreationOptions.Builder();

  /**
   * @see ActorCreationOptions.Builder#setResource(java.lang.String, java.lang.Double)
   */
  public T setResource(String resourceName, Double resourceQuantity) {
    builder.setResource(resourceName, resourceQuantity);
    return self();
  }

  /**
   * @see BaseActorCreator#setResources(java.util.Map)
   */
  public T setResources(Map<String, Double> resources) {
    builder.setResources(resources);
    return self();
  }

  /**
   * @see ActorCreationOptions.Builder#setMaxRestarts(int)
   */
  public T setMaxRestarts(int maxRestarts) {
    builder.setMaxRestarts(maxRestarts);
    return self();
  }

  /**
   * See {@link ActorCreationOptions.Builder#setMaxConcurrency(int)}
   */
  public T setMaxConcurrency(int maxConcurrency) {
    builder.setMaxConcurrency(maxConcurrency);
    return self();
  }

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }

  protected ActorCreationOptions buildOptions() {
    return builder.build();
  }

}
