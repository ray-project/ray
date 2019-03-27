package com.ray.streaming.schedule;

import com.ray.streaming.core.graph.ExecutionGraph;
import com.ray.streaming.core.runtime.StreamWorker;
import com.ray.streaming.plan.Plan;
import java.io.Serializable;
import java.util.List;
import org.ray.api.RayActor;

/**
 * Interface of the task assigning strategy.
 */
public interface ITaskAssign extends Serializable {

  /**
   * Assign logical plan to physical execution graph.
   */
  ExecutionGraph assign(Plan plan, List<RayActor<StreamWorker>> workers);

}
