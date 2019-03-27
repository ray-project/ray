package com.ray.streaming.schedule;


import com.ray.streaming.plan.Plan;

/**
 * Interface of the job scheduler.
 */
public interface IJobSchedule {

  /**
   * use ITaskAssign to assign logical plan to physical execution graph,
   * and schedule job to run.
   * @param plan logical plan.
   */
  void schedule(Plan plan);
}
