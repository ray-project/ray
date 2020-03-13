package org.ray.streaming.runtime.master.resourcemanager;

import java.util.List;
import java.util.Map;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.Resources;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategy;

/**
 * The resource manager is responsible for resource de-/allocation and monitoring ray cluster.
 */
public interface ResourceManager {

  /**
   * Get all registered container as a list.
   *
   * @return A list of containers.
   */
  List<Container> getRegisteredContainers();

  /**
   * Allocate resource to actor.
   *
   * @param container Specify the container to be allocated.
   * @param requireResource Resource size to be requested.
   * @return Allocated resource.
   */
  Map<String, Double> allocateResource(final Container container,
      final Map<String, Double> requireResource);

  /**
   * Deallocate resource to actor.
   *
   * @param container  Specify the container to be deallocate.
   * @param releaseResource Resource to be released.
   */
  void deallocateResource(final Container container,
      final Map<String, Double> releaseResource);

  /**
   * Get the current slot-assign strategy from manager.
   *
   * @return Current slot-assign strategy.
   */
  SlotAssignStrategy getSlotAssignStrategy();

  /**
   * Get resources from manager.
   *
   * @return Current resources in manager.
   */
  Resources getResources();
}