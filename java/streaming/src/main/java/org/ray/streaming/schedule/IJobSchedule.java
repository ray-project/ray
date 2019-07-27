package org.ray.streaming.schedule;


import org.ray.streaming.plan.Plan;

/**
 * Interface of the job scheduler.
 */
public interface IJobSchedule {

  /**
   * Assign logical plan to physical execution graph, and schedule job to run.
   *
   * @param plan The logical plan.
   */
  void schedule(Plan plan);
}
