package org.ray.streaming.schedule;


import java.util.Map;
import org.ray.streaming.jobgraph.JobGraph;

/**
 * Interface of the job scheduler.
 */
public interface JobScheduler {

  /**
   * Assign logical plan to physical execution graph, and schedule job to run.
   *
   * @param jobGraph The logical plan.
   */
  void schedule(JobGraph jobGraph, Map<String, String> conf);
}
