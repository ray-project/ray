package org.ray.streaming.runtime.master.resourcemanager.strategy;

import java.util.List;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.master.resourcemanager.ResourceAssignmentView;

/**
 * The ResourceAssignStrategy responsible assign {@link Container} to
 * {@link org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex}.
 */
public interface ResourceAssignStrategy {

  /**
   * Assign {@link Container} for {@link org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex}
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
