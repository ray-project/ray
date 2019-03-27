package org.ray.streaming.schedule;

import java.io.Serializable;
import java.util.List;
import org.ray.api.RayActor;
import org.ray.streaming.core.graph.ExecutionGraph;
import org.ray.streaming.core.runtime.StreamWorker;
import org.ray.streaming.plan.Plan;

/**
 * Interface of the task assigning strategy.
 */
public interface ITaskAssign extends Serializable {

  /**
   * Assign logical plan to physical execution graph.
   */
  ExecutionGraph assign(Plan plan, List<RayActor<StreamWorker>> workers);

}
