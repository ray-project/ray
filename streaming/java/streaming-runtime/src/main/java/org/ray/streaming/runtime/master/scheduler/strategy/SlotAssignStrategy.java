package org.ray.streaming.runtime.master.scheduler.strategy;

import java.util.List;
import java.util.Map;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.ContainerID;
import org.ray.streaming.runtime.core.resource.Resources;
import org.ray.streaming.runtime.core.resource.Slot;

/**
 * The SlotAssignStrategy managers a set of slots. When a container is
 * registered to ResourceManager, slots are assigned to it.
 */
public interface SlotAssignStrategy {

  /**
   * Calculate slot number per container and set to resources.
   */
  int getSlotNumPerContainer(List<Container> containers, int maxParallelism);

  /**
   * Allocate slot to container
   */
  void allocateSlot(final List<Container> containers, final int slotNumPerContainer);

  /**
   * Assign slot to execution vertex
   */
  Map<ContainerID, List<Slot>> assignSlot(ExecutionGraph executionGraph);

  /**
   * Get slot assign strategy name
   */
  String getName();

  /**
   * Set resources.
   */
  void setResources(Resources resources);
}
