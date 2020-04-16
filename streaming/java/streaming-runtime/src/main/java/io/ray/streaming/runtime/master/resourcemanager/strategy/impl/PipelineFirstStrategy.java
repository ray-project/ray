package io.ray.streaming.runtime.master.resourcemanager.strategy.impl;

import io.ray.streaming.runtime.config.types.ResourceAssignStrategyType;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.core.resource.ResourceType;
import io.ray.streaming.runtime.master.resourcemanager.ResourceAssignmentView;
import io.ray.streaming.runtime.master.resourcemanager.ViewBuilder;
import io.ray.streaming.runtime.master.resourcemanager.strategy.ResourceAssignStrategy;
import io.ray.streaming.runtime.master.scheduler.ScheduleException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class PipelineFirstStrategy implements ResourceAssignStrategy {

  public static final Logger LOG = LoggerFactory.getLogger(PipelineFirstStrategy.class);

  private int currentContainerIndex = 0;

  /**
   *  Assign resource to each execution vertex in the given execution graph.
   *
   * @param containers registered containers
   * @param executionGraph execution graph
   * @return allocating map, key is container ID, value is list of vertextId, and contains vertices
   */
  @Override
  public ResourceAssignmentView assignResource(
      List<Container> containers,
      ExecutionGraph executionGraph) {

    Map<Integer, ExecutionJobVertex> vertices = executionGraph.getExecutionJobVertexMap();
    Map<Integer, Integer> vertexRemainingNum = new HashMap<>();

    vertices.forEach((k, v) -> {
      int size = v.getExecutionVertices().size();
      vertexRemainingNum.put(k, size);
    });
    int totalExecutionVerticesNum = vertexRemainingNum.values().stream()
        .mapToInt(Integer::intValue)
        .sum();
    int containerNum = containers.size();
    int capacityPerContainer = Math.max(totalExecutionVerticesNum / containerNum, 1);

    updateContainerCapacity(containers, capacityPerContainer);

    int enlargeCapacityThreshold = 0;
    boolean enlarged = false;
    if (capacityPerContainer * containerNum < totalExecutionVerticesNum) {
      enlargeCapacityThreshold = capacityPerContainer * containerNum;
      LOG.info("Need to enlarge capacity per container, threshold: {}.", enlargeCapacityThreshold);
    }
    LOG.info("Total execution vertices num: {}, container num: {}, capacity per container: {}.",
        totalExecutionVerticesNum, containerNum, capacityPerContainer);

    int maxParallelism = executionGraph.getMaxParallelism();

    int allocatedVertexCount = 0;
    for (int i = 0; i < maxParallelism; i++) {
      for (ExecutionJobVertex jobVertex : vertices.values()) {
        List<ExecutionVertex> exeVertices = jobVertex.getExecutionVertices();
        // current job vertex assign finished
        if (exeVertices.size() <= i) {
          continue;
        }
        ExecutionVertex executionVertex = exeVertices.get(i);
        Map<String, Double> requiredResource = executionVertex.getResources();
        if (requiredResource.containsKey(ResourceType.CPU.getValue())) {
          LOG.info("Required resource contain {} value : {}, no limitation by default.",
              ResourceType.CPU, requiredResource.get(ResourceType.CPU.getValue()));
          requiredResource.remove(ResourceType.CPU.getValue());
        }

        Container targetContainer = findMatchedContainer(requiredResource, containers);

        targetContainer.allocateActor(executionVertex);
        allocatedVertexCount++;
        // Once allocatedVertexCount reaches threshold, we should enlarge capacity
        if (!enlarged && enlargeCapacityThreshold > 0
            && allocatedVertexCount >= enlargeCapacityThreshold) {
          updateContainerCapacity(containers, capacityPerContainer + 1);
          enlarged = true;
          LOG.info("Enlarge capacity per container to: {}.", containers.get(0).getCapacity());
        }
      }
    }

    ResourceAssignmentView allocatingView = ViewBuilder.buildResourceAssignmentView(containers);
    LOG.info("Assigning resource finished, allocating map: {}.", allocatingView);
    return allocatingView;
  }

  @Override
  public String getName() {
    return ResourceAssignStrategyType.PIPELINE_FIRST_STRATEGY.getName();
  }

  /**
   * Update container capacity. eg: we have 89 actors and 8 containers, capacity will be 11 when
   * initialing, and will be increased to 12 when allocating actor#89, just for load balancing.
   */
  private void updateContainerCapacity(List<Container> containers, int capacity) {
    containers.forEach(c -> c.updateCapacity(capacity));
  }

  /**
   * Find a container which matches required resource
   * @param requiredResource required resource
   * @param containers registered containers
   * @return container that matches the required resource
   */
  private Container findMatchedContainer(
      Map<String, Double> requiredResource,
      List<Container> containers) {

    LOG.info("Check resource, required: {}.", requiredResource);

    int checkedNum = 0;
    // if current container does not have enough resource, go to the next one (loop)
    while (!hasEnoughResource(requiredResource, getCurrentContainer(containers))) {
      checkedNum++;
      forwardToNextContainer(containers);
      if (checkedNum >= containers.size()) {
        throw new ScheduleException(
            String.format("No enough resource left, required resource: %s, available resource: %s.",
                requiredResource, containers));
      }
    }
    return getCurrentContainer(containers);
  }

  /**
   * Check if current container has enough resource
   * @param requiredResource required resource
   * @param container container
   * @return true if matches, false else
   */
  private boolean hasEnoughResource(Map<String, Double> requiredResource, Container container) {
    LOG.info("Check resource for index: {}, container: {}", currentContainerIndex, container);

    if (null == requiredResource) {
      return true;
    }

    if (container.isFull()) {
      LOG.info("Container {} is full.", container);
      return false;
    }

    Map<String, Double> availableResource = container.getAvailableResources();
    for (Map.Entry<String, Double> entry : requiredResource.entrySet()) {
      if (availableResource.containsKey(entry.getKey())) {
        if (availableResource.get(entry.getKey()) < entry.getValue()) {
          LOG.warn("No enough resource for container {}. required: {}, available: {}.",
              container.getAddress(), requiredResource, availableResource);
          return false;
        }
      } else {
        LOG.warn("No enough resource for container {}. required: {}, available: {}.",
            container.getAddress(), requiredResource, availableResource);
        return false;
      }
    }

    return true;
  }

  /**
   * Forward to next container
   *
   * @param containers registered container list
   * @return next container in the list
   */
  private Container forwardToNextContainer(List<Container> containers) {
    this.currentContainerIndex = (this.currentContainerIndex + 1) % containers.size();
    return getCurrentContainer(containers);
  }

  /**
   * Get current container
   * @param containers registered container
   * @return current container to allocate actor
   */
  private Container getCurrentContainer(List<Container> containers) {
    return containers.get(currentContainerIndex);
  }
}
