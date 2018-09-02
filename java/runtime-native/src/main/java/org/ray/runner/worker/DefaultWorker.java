package org.ray.runner.worker;

import org.ray.core.AbstractRayRuntime;
import org.ray.core.model.WorkerMode;

/**
 * default worker implementation.
 */
public class DefaultWorker {

  //
  // String workerCmd = "java" + " -jarls " + workerPath + " --node-ip-address=" + ip
  // + " --object-store-name=" + storeName
  // + " --object-store-manager-name=" + storeManagerName
  // + " --local-scheduler-name=" + name + " --redis-address=" + redisAddress
  //
  public static void main(String[] args) {
    try {
      AbstractRayRuntime.init(args);
      assert AbstractRayRuntime.getParams().worker_mode == WorkerMode.WORKER;
      AbstractRayRuntime.getInstance().loop();
      throw new RuntimeException("Control flow should never reach here");

    } catch (Throwable e) {
      e.printStackTrace();
      System.err
          .println("--config=ray.config.ini --overwrite=ray.java.start.worker_mode=WORKER;...");
      System.exit(-1);
    }
  }
}
