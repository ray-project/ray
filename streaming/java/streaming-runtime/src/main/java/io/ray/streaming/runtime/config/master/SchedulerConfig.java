package io.ray.streaming.runtime.config.master;

import io.ray.streaming.runtime.config.Config;

/** Configuration for job scheduler. */
public interface SchedulerConfig extends Config {

  String WORKER_INITIATION_WAIT_TIMEOUT_MS = "streaming.scheduler.worker.initiation.timeout.ms";
  String WORKER_STARTING_WAIT_TIMEOUT_MS = "streaming.scheduler.worker.starting.timeout.ms";

  /**
   * The timeout ms of worker initiation. Default is: 10000ms(10s).
   *
   * <p>Returns timeout ms
   */
  @Key(WORKER_INITIATION_WAIT_TIMEOUT_MS)
  @DefaultValue(value = "10000")
  int workerInitiationWaitTimeoutMs();

  /**
   * The timeout ms of worker starting. Default is: 10000ms(10s).
   *
   * <p>Returns timeout ms
   */
  @Key(WORKER_STARTING_WAIT_TIMEOUT_MS)
  @DefaultValue(value = "10000")
  int workerStartingWaitTimeoutMs();
}
