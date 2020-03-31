package io.ray.streaming.runtime.schedule;

import java.io.Serializable;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.runtime.core.graph.ExecutionGraph;

/**
 * Interface of the task assigning strategy.
 */
public interface TaskAssigner extends Serializable {

  /**
   * Assign logical plan to physical execution graph.
   */
  ExecutionGraph assign(JobGraph jobGraph);

}
