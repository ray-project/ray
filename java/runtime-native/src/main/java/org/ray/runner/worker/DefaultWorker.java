package org.ray.runner.worker;

import org.ray.core.RayRuntime;
import org.ray.core.model.WorkerMode;

/**
 * default worker implementation
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
      RayRuntime.init(args);
      assert RayRuntime.getParams().worker_mode == WorkerMode.WORKER;
      RayRuntime.getInstance().loop();
      throw new RuntimeException("Control flow should never reach here");

    } catch (Throwable e) {
      e.printStackTrace();
      System.err
          .println("--config=ray.config.ini --overwrite=ray.java.start.worker_mode=WORKER;...");
      System.exit(-1);
    }
  }
}
