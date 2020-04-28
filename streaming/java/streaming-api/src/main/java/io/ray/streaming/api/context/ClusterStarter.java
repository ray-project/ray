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
  private static final String PLASMA_STORE_SOCKET_NAME = "/tmp/ray/plasma_store_socket";
  private static final String RAYLET_SOCKET_NAME = "/tmp/ray/raylet_socket";

  static synchronized void startCluster(boolean isCrossLanguage, boolean isLocal) {
    Preconditions.checkArgument(Ray.internal() == null);
    RayConfig.reset();
    if (!isLocal) {
      System.setProperty("ray.raylet.config.num_workers_per_process_java", "1");
      System.setProperty("ray.run-mode", "CLUSTER");
    } else {
      System.clearProperty("ray.raylet.config.num_workers_per_process_java");
      System.setProperty("ray.run-mode", "SINGLE_PROCESS");
    }

    if (!isCrossLanguage) {
      Ray.init();
      return;
    }

    // Delete existing socket files.
    for (String socket : ImmutableList.of(RAYLET_SOCKET_NAME, PLASMA_STORE_SOCKET_NAME)) {
      File file = new File(socket);
      if (file.exists()) {
        LOG.info("Delete existing socket file {}", file);
        file.delete();
      }
    }

    String nodeManagerPort = String.valueOf(NetworkUtil.getUnusedPort());

    // jars in the `ray` wheel doesn't contains test classes, so we add test classes explicitly.
    // Since mvn test classes contains `test` in path and bazel test classes is located at a jar
    // with `test` included in the name, we can check classpath `test` to filter out test classes.
    String classpath = Stream.of(System.getProperty("java.class.path").split(":"))
        .filter(s -> !s.contains(" ") && s.contains("test"))
        .collect(Collectors.joining(":"));
    String workerOptions = new Gson().toJson(ImmutableList.of("-classpath", classpath));
    Map<String, String> config = new HashMap<>(RayConfig.create().rayletConfigParameters);
    config.put("num_workers_per_process_java", "1");
    // Start ray cluster.
    List<String> startCommand = ImmutableList.of(
        "ray",
        "start",
        "--head",
        "--redis-port=6379",
        String.format("--plasma-store-socket-name=%s", PLASMA_STORE_SOCKET_NAME),
        String.format("--raylet-socket-name=%s", RAYLET_SOCKET_NAME),
        String.format("--node-manager-port=%s", nodeManagerPort),
        "--load-code-from-local",
        "--include-java",
        "--java-worker-options=" + workerOptions,
        "--internal-config=" + new Gson().toJson(config)
    );
    if (!executeCommand(startCommand, 10)) {
      throw new RuntimeException("Couldn't start ray cluster.");
    }

    // Connect to the cluster.
    System.setProperty("ray.redis.address", "127.0.0.1:6379");
    System.setProperty("ray.object-store.socket-name", PLASMA_STORE_SOCKET_NAME);
    System.setProperty("ray.raylet.socket-name", RAYLET_SOCKET_NAME);
    System.setProperty("ray.raylet.node-manager-port", nodeManagerPort);
    Ray.init();
  }

  public static synchronized void stopCluster(boolean isCrossLanguage) {
    // Disconnect to the cluster.
    Ray.shutdown();
    System.clearProperty("ray.redis.address");
    System.clearProperty("ray.object-store.socket-name");
    System.clearProperty("ray.raylet.socket-name");
    System.clearProperty("ray.raylet.node-manager-port");
    System.clearProperty("ray.raylet.config.num_workers_per_process_java");
    System.clearProperty("ray.run-mode");

    if (isCrossLanguage) {
      // Stop ray cluster.
      final List<String> stopCommand = ImmutableList.of(
          "ray",
          "stop"
      );
      if (!executeCommand(stopCommand, 10)) {
        throw new RuntimeException("Couldn't stop ray cluster");
      }
    }
  }

  /**
   * Execute an external command.
   *
   * @return Whether the command succeeded.
   */
  private static boolean executeCommand(List<String> command, int waitTimeoutSeconds) {
    LOG.info("Executing command: {}", String.join(" ", command));
    try {
      ProcessBuilder processBuilder = new ProcessBuilder(command)
          .redirectOutput(ProcessBuilder.Redirect.INHERIT)
          .redirectError(ProcessBuilder.Redirect.INHERIT);
      Process process = processBuilder.start();
      boolean exit = process.waitFor(waitTimeoutSeconds, TimeUnit.SECONDS);
      if (!exit) {
        process.destroyForcibly();
      }
      return process.exitValue() == 0;
    } catch (Exception e) {
      throw new RuntimeException("Error executing command " + String.join(" ", command), e);
    }
  }
}
