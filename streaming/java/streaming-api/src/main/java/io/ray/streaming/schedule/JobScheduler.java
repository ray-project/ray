package io.ray.streaming.schedule;


import io.ray.streaming.jobgraph.JobGraph;
import java.util.Map;

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
