package org.ray.streaming.runtime.core.master.resourcemanager;

import java.util.List;
import java.util.Map;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.master.scheduler.strategy.SlotAssignStrategy;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.Resources;

/**
 * The central role of resource management in JobMaster.
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
   * @param exeVertex The specified worker vertex.
   * @return Allocated resource.
   */
  Map<String, Double> allocateResource(final ExecutionVertex exeVertex);

  /**
   * Deallocate resource to actor.
   *
   * @param exeVertex The specified worker vertex.
   */
  void deallocateResource(final ExecutionVertex exeVertex);

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