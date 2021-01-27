package io.ray.streaming.api.context;

import com.google.common.base.Preconditions;
import io.ray.api.Ray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClusterStarter {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterStarter.class);

  static synchronized void startCluster(boolean isLocal) {
    Preconditions.checkArgument(!Ray.isInitialized());
    if (!isLocal) {
      System.setProperty("ray.run-mode", "CLUSTER");
    } else {
      System.setProperty("ray.run-mode", "SINGLE_PROCESS");
    }

    Ray.init();
  }

  public static synchronized void stopCluster() {
    // Disconnect to the cluster.
    Ray.shutdown();
    System.clearProperty("ray.run-mode");
  }
}
