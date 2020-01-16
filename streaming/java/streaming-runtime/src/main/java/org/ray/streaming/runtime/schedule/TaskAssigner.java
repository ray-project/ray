package org.ray.streaming.runtime.schedule;

import java.io.Serializable;
import java.util.List;
import org.ray.api.RayActor;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.runtime.core.graph.ExecutionGraph;
import org.ray.streaming.runtime.worker.JobWorker;

/**
 * Interface of the task assigning strategy.
 */
public interface TaskAssigner extends Serializable {

  /**
   * Assign logical plan to physical execution graph.
   */
  ExecutionGraph assign(JobGraph jobGraph, List<RayActor<JobWorker>> workers);

}
