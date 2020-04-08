package io.ray.streaming.runtime.schedule;

import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.runtime.core.graph.ExecutionGraph;
import java.io.Serializable;

/**
 * Interface of the task assigning strategy.
 */
public interface TaskAssigner extends Serializable {

  /**
   * Assign logical plan to physical execution graph.
   */
  ExecutionGraph assign(JobGraph jobGraph);

}
