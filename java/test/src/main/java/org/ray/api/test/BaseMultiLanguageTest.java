package org.ray.api.test;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.lang.ProcessBuilder.Redirect;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.ray.api.Ray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

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

  private void checkMultiLanguageTestFlag() {
    if (!"1".equals(System.getenv("ENABLE_MULTI_LANGUAGE_TESTS"))) {
      LOGGER.info("Skip Multi-language tests because environment variable "
          + "ENABLE_MULTI_LANGUAGE_TESTS isn't set");
      throw new SkipException("Skip test.");
    }
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    checkMultiLanguageTestFlag();

    // Delete existing socket files.
    for (String socket : ImmutableList.of(RAYLET_SOCKET_NAME, PLASMA_STORE_SOCKET_NAME)) {
      File file = new File(socket);
      if (file.exists()) {
        file.delete();
      }
    }

    // Start ray cluster.
    String workerOptions =
        " -classpath " + System.getProperty("java.class.path");
    List<String> startCommand = ImmutableList.of(
        "ray",
        "start",
        "--head",
        "--redis-port=6379",
        String.format("--plasma-store-socket-name=%s", PLASMA_STORE_SOCKET_NAME),
        String.format("--raylet-socket-name=%s", RAYLET_SOCKET_NAME),
        "--load-code-from-local",
        "--include-java",
        "--java-worker-options=" + workerOptions
    );
    String numWorkersPerProcessJava = System
        .getProperty("ray.raylet.config.num_workers_per_process_java");
    if (!Strings.isNullOrEmpty(numWorkersPerProcessJava)) {
      startCommand = ImmutableList.<String>builder().addAll(startCommand)
          .add(String.format("--internal-config={\"num_workers_per_process_java\": %s}",
              numWorkersPerProcessJava)).build();
    }
    if (!executeCommand(startCommand, 10, getRayStartEnv())) {
      throw new RuntimeException("Couldn't start ray cluster.");
    }

    // Connect to the cluster.
    System.setProperty("ray.redis.address", "127.0.0.1:6379");
    System.setProperty("ray.object-store.socket-name", PLASMA_STORE_SOCKET_NAME);
    System.setProperty("ray.raylet.socket-name", RAYLET_SOCKET_NAME);
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
    checkMultiLanguageTestFlag();

    // Disconnect to the cluster.
    Ray.shutdown();
    System.clearProperty("ray.redis.address");
    System.clearProperty("ray.object-store.socket-name");
    System.clearProperty("ray.raylet.socket-name");

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
