package io.ray.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.ray.api.Ray;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.util.NetworkUtil;
import java.io.File;
import java.lang.ProcessBuilder.Redirect;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster", "multiLanguage"})
public abstract class BaseMultiLanguageTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseMultiLanguageTest.class);

  private static final String PLASMA_STORE_SOCKET_NAME = "/tmp/ray/test/plasma_store_socket";
  private static final String RAYLET_SOCKET_NAME = "/tmp/ray/test/raylet_socket";

  /**
   * Execute an external command.
   *
   * @return Whether the command succeeded.
   */
  private boolean executeCommand(List<String> command, int waitTimeoutSeconds,
      Map<String, String> env) {
    try {
      LOGGER.info("Executing command: {}", String.join(" ", command));
      ProcessBuilder processBuilder = new ProcessBuilder(command).redirectOutput(Redirect.INHERIT)
          .redirectError(Redirect.INHERIT);
      for (Entry<String, String> entry : env.entrySet()) {
        processBuilder.environment().put(entry.getKey(), entry.getValue());
      }
      Process process = processBuilder.start();
      process.waitFor(waitTimeoutSeconds, TimeUnit.SECONDS);
      return process.exitValue() == 0;
    } catch (Exception e) {
      throw new RuntimeException("Error executing command " + String.join(" ", command), e);
    }
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    // Delete existing socket files.
    for (String socket : ImmutableList.of(RAYLET_SOCKET_NAME, PLASMA_STORE_SOCKET_NAME)) {
      File file = new File(socket);
      if (file.exists()) {
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
    // Start ray cluster.
    List<String> startCommand = ImmutableList.of(
        "ray",
        "start",
        "--head",
        "--redis-port=6379",
        "--min-worker-port=0",
        "--max-worker-port=0",
        String.format("--plasma-store-socket-name=%s", PLASMA_STORE_SOCKET_NAME),
        String.format("--raylet-socket-name=%s", RAYLET_SOCKET_NAME),
        String.format("--node-manager-port=%s", nodeManagerPort),
        "--load-code-from-local",
        "--include-java",
        "--java-worker-options=" + workerOptions,
        "--internal-config=" + new Gson().toJson(RayConfig.create().rayletConfigParameters)
    );
    if (!executeCommand(startCommand, 10, getRayStartEnv())) {
      throw new RuntimeException("Couldn't start ray cluster.");
    }

    // Connect to the cluster.
    Assert.assertNull(Ray.internal());
    System.setProperty("ray.redis.address", "127.0.0.1:6379");
    System.setProperty("ray.object-store.socket-name", PLASMA_STORE_SOCKET_NAME);
    System.setProperty("ray.raylet.socket-name", RAYLET_SOCKET_NAME);
    System.setProperty("ray.raylet.node-manager-port", nodeManagerPort);
    Ray.init();
  }

  /**
   * @return The environment variables needed for the `ray start` command.
   */
  protected Map<String, String> getRayStartEnv() {
    return ImmutableMap.of();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    // Disconnect to the cluster.
    Ray.shutdown();
    System.clearProperty("ray.redis.address");
    System.clearProperty("ray.object-store.socket-name");
    System.clearProperty("ray.raylet.socket-name");
    System.clearProperty("ray.raylet.node-manager-port");

    // Stop ray cluster.
    final List<String> stopCommand = ImmutableList.of(
        "ray",
        "stop"
    );
    if (!executeCommand(stopCommand, 10, ImmutableMap.of())) {
      throw new RuntimeException("Couldn't stop ray cluster");
    }
  }
}
