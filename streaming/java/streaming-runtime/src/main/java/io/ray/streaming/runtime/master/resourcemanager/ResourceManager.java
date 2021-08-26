package io.ray.streaming.runtime.master.resourcemanager;

import com.google.common.collect.ImmutableList;
import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.master.resourcemanager.strategy.ResourceAssignStrategy;

/** ResourceManager(RM) is responsible for resource de-/allocation and monitoring ray cluster. */
public interface ResourceManager extends ResourceAssignStrategy {

  /**
   * Get registered containers, the container list is read-only.
   *
   * @return the registered container list
   */
  ImmutableList<Container> getRegisteredContainers();
}
