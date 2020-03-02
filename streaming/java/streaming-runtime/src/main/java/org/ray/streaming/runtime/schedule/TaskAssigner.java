package org.ray.streaming.runtime.schedule;

import java.io.Serializable;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.runtime.core.graph.ExecutionGraph;

/**
 * Interface of the task assigning strategy.
 */
public interface TaskAssigner extends Serializable {

  /**
   * Assign logical plan to physical execution graph.
   */
  ExecutionGraph assign(JobGraph jobGraph);

}
