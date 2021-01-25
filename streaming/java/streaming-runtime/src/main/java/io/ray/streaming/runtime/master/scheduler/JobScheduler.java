package io.ray.streaming.runtime.master.scheduler;

import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;

/** Job scheduler is used to do the scheduling in JobMaster. */
public interface JobScheduler {

  /**
   * Schedule streaming job using the physical plan.
   *
   * @param executionGraph physical plan
   * @return scheduling result
   */
  boolean scheduleJob(ExecutionGraph executionGraph);
}
