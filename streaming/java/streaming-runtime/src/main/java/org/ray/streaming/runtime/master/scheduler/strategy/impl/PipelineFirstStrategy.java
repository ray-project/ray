package org.ray.streaming.runtime.master.scheduler.strategy.impl;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.streaming.runtime.config.types.SlotAssignStrategyType;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.ContainerID;
import org.ray.streaming.runtime.core.resource.Resources;
import org.ray.streaming.runtime.core.resource.Slot;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Based on Ray dynamic resource function, resource details(by ray gcs get) and
 * execution logic diagram, PipelineFirstStrategy provides a actor scheduling
 * strategies to make the cluster load balanced and controllable scheduling.
 * Assume that we have 2 containers and have a DAG graph composed of a source node with parallelism
 * of 2 and a sink node with parallelism of 2, the structure will be like:
 * <pre>
 *   container_0
 *             |- source_1
 *             |- sink_1
 *   container_1
 *             |- source_2
 *             |- sink_2
 * </pre>
 */
public class PipelineFirstStrategy implements SlotAssignStrategy {

  public static final Logger LOG = LoggerFactory.getLogger(PipelineFirstStrategy.class);

  protected Resources resources;

  @Override
  public int getSlotNumPerContainer(List<Container> containers, int maxParallelism) {
    LOG.info("max parallelism: {}, container size: {}.", maxParallelism, containers.size());
    int slotNumPerContainer =
        (int) Math.ceil(Math.max(maxParallelism, containers.size()) * 1.0 / containers.size());
    LOG.info("slot num per container: {}.", slotNumPerContainer);
    return slotNumPerContainer;
  }

  /**
   * Allocate slot to target container, assume that we have 2 containers and max parallelism is 5,
   * the structure will be like:
   * <pre>
   * container_0
   *           |- slot_0
   *           |- slot_2
   *           |- slot_4
   * container_1
   *           |- slot_1
   *           |- slot_3
   *           |- slot_5
   * </pre>
   */
  @Override
  public void allocateSlot(List<Container> containers,
      int slotNumPerContainer) {
    int maxSlotSize = containers.size() * slotNumPerContainer;
    LOG.info("Allocate slot, maxSlotSize: {}.", maxSlotSize);

    for (int slotId = 0; slotId < maxSlotSize; ++slotId) {
      Container targetContainer = containers.get(slotId % containers.size());
      Slot slot = new Slot(slotId, targetContainer.getContainerId());
      targetContainer.getSlots().add(slot);
    }

    // update new added containers' allocating map
    containers.forEach(c -> {
      List<Slot> slots = c.getSlots();
      resources.getAllocatingMap().put(c.getContainerId(), slots);
    });

    LOG.info("Allocate slot result: {}.", resources.getAllocatingMap());
  }

  @Override
  public Map<ContainerID, List<Slot>> assignSlot(ExecutionGraph executionGraph) {
    LOG.info("Container available resources: {}.", resources.getAllAvailableResource());
    Map<Integer, ExecutionJobVertex> vertices = executionGraph.getExecutionJobVertexMap();
    Map<Integer, Integer> vertexRemainingNum = new HashMap<>();
    vertices.forEach((k, v) -> {
      int size = v.getExecutionVertices().size();
      vertexRemainingNum.put(k, size);
    });
    int totalExecutionVerticesNum = vertexRemainingNum.values().stream()
        .mapToInt(Integer::intValue)
        .sum();
    int containerNum = resources.getRegisterContainers().size();
    resources.setActorPerContainer((int) Math
        .ceil(totalExecutionVerticesNum * 1.0 / containerNum));
    LOG.info("Total execution vertices num: {}, container num: {}, capacity per container: {}.",
        totalExecutionVerticesNum, containerNum, resources.getActorPerContainer());

    int maxParallelism = executionGraph.getMaxParallelism();

    for (int i = 0; i < maxParallelism; i++) {
      for (ExecutionJobVertex executionJobVertex : vertices.values()) {
        List<ExecutionVertex> exeVertices = executionJobVertex.getExecutionVertices();
        // current job vertex assign finished
        if (exeVertices.size() <= i) {
          continue;
        }

        ExecutionVertex executionVertex = exeVertices.get(i);

        //check current container has enough resources.
        checkResource(executionVertex.getResources());

        Container targetContainer = resources.getRegisterContainers()
            .get(resources.getCurrentContainerIndex());
        List<Slot> targetSlots = targetContainer.getSlots();
        allocate(executionVertex, targetContainer, targetSlots.get(i % targetSlots.size()));
      }
    }

    return resources.getAllocatingMap();
  }

  private void checkResource(Map<String, Double> requiredResource) {
    int checkedNum = 0;
    // if current container does not have enough resource, go to the next one (loop)
    while (!hasEnoughResource(requiredResource)) {
      checkedNum++;
      resources.setCurrentContainerIndex((resources.getCurrentContainerIndex() + 1) %
          resources.getRegisterContainers().size());

      Preconditions.checkArgument(checkedNum < resources.getRegisterContainers().size(),
          "No enough resource left, required resource: {}, available resource: {}.",
          requiredResource, resources.getAllAvailableResource());
      resources.setCurrentContainerAllocatedActorNum(0);
    }
  }

  private boolean hasEnoughResource(Map<String, Double> requiredResource) {
    LOG.info("Check resource for container, index: {}.", resources.getCurrentContainerIndex());

    if (null == requiredResource) {
      return true;
    }

    Container currentContainer = resources.getRegisterContainers()
        .get(resources.getCurrentContainerIndex());
    List<Slot> slotActors = resources.getAllocatingMap().get(currentContainer.getContainerId());
    if (slotActors != null && slotActors.size() > 0) {
      long allocatedActorNum = slotActors.stream()
          .map(Slot::getExecutionVertexIds)
          .mapToLong(List::size)
          .sum();
      if (allocatedActorNum  >= resources.getActorPerContainer()) {
        LOG.info("Container remaining capacity is 0. used: {}, total: {}.", allocatedActorNum,
            resources.getActorPerContainer());
        return false;
      }
    }

    Map<String, Double> availableResource = currentContainer.getAvailableResource();
    for (Map.Entry<String, Double> entry : requiredResource.entrySet()) {
      if (availableResource.containsKey(entry.getKey())) {
        if (availableResource.get(entry.getKey()) < entry.getValue()) {
          LOG.warn("No enough resource for container {}. required: {}, available: {}.",
              currentContainer.getAddress(), requiredResource, availableResource);
          return false;
        }
      }
    }
    return true;
  }

  private void allocate(ExecutionVertex vertex, Container container, Slot slot) {
    // set slot for execution vertex
    LOG.info("Set slot {} to vertex {}.", slot, vertex);
    vertex.setSlotIfNotExist(slot);

    Slot useSlot = resources.getAllocatingMap().get(container.getContainerId())
        .stream().filter(s -> s.getId() == slot.getId()).findFirst().get();
    useSlot.getExecutionVertexIds().add(vertex.getVertexId());

    // current container reaches capacity limitation, go to the next one.
    resources.setCurrentContainerAllocatedActorNum(
        resources.getCurrentContainerAllocatedActorNum() + 1);
    if (resources.getCurrentContainerAllocatedActorNum() >= resources.getActorPerContainer()) {
      resources.setCurrentContainerIndex(
          (resources.getCurrentContainerIndex() + 1) % resources.getRegisterContainers().size());
      resources.setCurrentContainerAllocatedActorNum(0);
    }
  }

  @Override
  public String getName() {
    return SlotAssignStrategyType.PIPELINE_FIRST_STRATEGY.getValue();
  }

  @Override
  public void setResources(Resources resources) {
    this.resources = resources;
  }
}
