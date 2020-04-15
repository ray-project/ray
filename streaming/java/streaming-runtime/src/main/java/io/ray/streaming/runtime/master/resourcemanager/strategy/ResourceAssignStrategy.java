package io.ray.streaming.runtime.master.resourcemanager.strategy;

import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.master.resourcemanager.ResourceAssignmentView;
import java.util.List;

/**
 * The ResourceAssignStrategy responsible assign {@link io.ray.streaming.runtime.core.resource.Container} to
 * {@link io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex}.
 */
public interface ResourceAssignStrategy {

  /**
   * Assign {@link io.ray.streaming.runtime.core.resource.Container} for
   * {@link io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex}
   *
   * @param containers registered container
   * @param executionGraph execution graph
   * @return allocating view
   */
  ResourceAssignmentView assignResource(List<Container> containers, ExecutionGraph executionGraph);


  /**
   * Get container assign strategy name
   */
  String getName();

}
