package com.ray.streaming.schedule;


import com.ray.streaming.plan.Plan;

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
