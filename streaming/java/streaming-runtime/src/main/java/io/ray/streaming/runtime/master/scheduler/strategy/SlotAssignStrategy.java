package io.ray.streaming.runtime.master.scheduler.strategy;

import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.core.resource.ContainerID;
import io.ray.streaming.runtime.core.resource.Resources;
import io.ray.streaming.runtime.core.resource.Slot;
import java.util.List;
import java.util.Map;

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
