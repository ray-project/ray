package io.ray.streaming.runtime.master.resourcemanager.strategy;

import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.master.resourcemanager.ResourceAssignmentView;
import java.util.List;

/** The ResourceAssignStrategy responsible assign {@link Container} to {@link ExecutionVertex}. */
public interface ResourceAssignStrategy {

  /**
   * Assign {@link Container} for {@link ExecutionVertex}
   *
   * @param containers registered container
   * @param executionGraph execution graph Returns allocating view
   */
  ResourceAssignmentView assignResource(List<Container> containers, ExecutionGraph executionGraph);

  /** Get container assign strategy name */
  String getName();
}
