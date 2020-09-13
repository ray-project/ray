package io.ray.streaming.api.context;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import io.ray.api.Ray;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.util.NetworkUtil;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClusterStarter {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterStarter.class);

  static synchronized void startCluster(boolean isLocal) {
    Preconditions.checkArgument(!Ray.isInitialized());
    if (!isLocal) {
      System.setProperty("ray.raylet.config.num_workers_per_process_java", "1");
      System.setProperty("ray.run-mode", "CLUSTER");
    } else {
      System.clearProperty("ray.raylet.config.num_workers_per_process_java");
      System.setProperty("ray.run-mode", "SINGLE_PROCESS");
    }

    Ray.init();
  }

  public static synchronized void stopCluster() {
    // Disconnect to the cluster.
    Ray.shutdown();
    System.clearProperty("ray.raylet.config.num_workers_per_process_java");
    System.clearProperty("ray.run-mode");
  }
}
