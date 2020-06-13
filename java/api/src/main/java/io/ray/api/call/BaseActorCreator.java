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
   * Set a custom resource requirement to reserve for the lifetime of this actor.
   * This method can be called multiple times. If the same resource is set multiple times,
   * the latest quantity will be used.
   *
   * @param resourceName resource name
   * @param resourceQuantity resource quantity
   * @return self
   * @see ActorCreationOptions.Builder#setResource(java.lang.String, java.lang.Double)
   */
  public T setResource(String resourceName, Double resourceQuantity) {
    builder.setResource(resourceName, resourceQuantity);
    return self();
  }

  /**
   * Set custom resource requirements to reserve for the lifetime of this actor.
   * This method can be called multiple times. If the same resource is set multiple times,
   * the latest quantity will be used.
   *
   * @param resources requirements for multiple resources.
   * @return self
   * @see BaseActorCreator#setResources(java.util.Map)
   */
  public T setResources(Map<String, Double> resources) {
    builder.setResources(resources);
    return self();
  }

  /**
   * This specifies the maximum number of times that the actor should be restarted when it dies
   * unexpectedly. The minimum valid value is 0 (default), which indicates that the actor doesn't
   * need to be restarted. A value of -1 indicates that an actor should be restarted indefinitely.
   *
   * @param maxRestarts max number of actor restarts
   * @return self
   * @see ActorCreationOptions.Builder#setMaxRestarts(int)
   */
  public T setMaxRestarts(int maxRestarts) {
    builder.setMaxRestarts(maxRestarts);
    return self();
  }

  /**
   /**
   * Set the max number of concurrent calls to allow for this actor.
   *
   * The max concurrency defaults to 1 for threaded execution.
   * Note that the execution order is not guaranteed when max_concurrency > 1.
   *
   * @param maxConcurrency The max number of concurrent calls to allow for this actor.
   * @return self
   * @see ActorCreationOptions.Builder#setMaxConcurrency(int)
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
