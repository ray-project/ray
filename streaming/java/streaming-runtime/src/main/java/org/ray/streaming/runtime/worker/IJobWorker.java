package org.ray.streaming.runtime.worker;

import org.ray.streaming.runtime.worker.context.JobWorkerContext;

/**
 * The stream worker interface.
 */
public interface IJobWorker {

  /**
   * Init job worker with context.
   * @param workerContext worker context.
   * @return Init result.
   */
  Boolean init(JobWorkerContext workerContext);

  /**
   * Start job worker working progress.
   * @return Start result.
   */
  Boolean start();

  /**
   * Shutdown worker in purpose (Will failover by ray).
   */
  void shutdown();

  /**
   * Destroy worker (Won't failover by ray).
   * @return Destroy result.
   */
  Boolean destroy();
}
