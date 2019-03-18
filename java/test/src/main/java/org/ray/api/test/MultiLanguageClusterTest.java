package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test starting a ray cluster with multi-language support.
 */
public class MultiLanguageClusterTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiLanguageClusterTest.class);

  private static final String PLASMA_STORE_SOCKET_NAME = "/tmp/ray/test/plasma_store_socket";
  private static final String RAYLET_SOCKET_NAME = "/tmp/ray/test/raylet_socket";

  @RayRemote
  public static String echo(String word) {
    return word;
  }

  /**
   * Execute an external command.
   *
   * @return Whether the command succeeded.
   */
  private boolean executeCommand(List<String> command, int waitTimeoutSeconds) {
    try {
      LOGGER.info("Executing command: {}", String.join(" ", command));
      Process process = new ProcessBuilder(command).inheritIO().start();
      process.waitFor(waitTimeoutSeconds, TimeUnit.SECONDS);
      return process.exitValue() == 0;
    } catch (Exception e) {
      throw new RuntimeException("Error executing command " + String.join(" ", command), e);
    }
  }

  @BeforeMethod
  public void setUp(Method method) {
    String testName = method.getName();
    if (!"1".equals(System.getenv("ENABLE_MULTI_LANGUAGE_TESTS"))) {
      LOGGER.info("Skip " + testName +
          " because env variable ENABLE_MULTI_LANGUAGE_TESTS isn't set");
      throw new SkipException("Skip test.");
    }

    // Delete existing socket files.
    for (String socket : ImmutableList.of(RAYLET_SOCKET_NAME, PLASMA_STORE_SOCKET_NAME)) {
      File file = new File(socket);
      if (file.exists()) {
        file.delete();
      }
    }

    // Start ray cluster.
    String testDir = System.getProperty("user.dir");
    String workerOptions =
        " -classpath " + String.format("%s/../../build/java/*:%s/target/*", testDir, testDir);
    final List<String> startCommand = ImmutableList.of(
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
    if (!executeCommand(startCommand, 10)) {
      throw new RuntimeException("Couldn't start ray cluster.");
    }

    // Connect to the cluster.
    System.setProperty("ray.redis.address", "127.0.0.1:6379");
    System.setProperty("ray.object-store.socket-name", PLASMA_STORE_SOCKET_NAME);
    System.setProperty("ray.raylet.socket-name", RAYLET_SOCKET_NAME);
    Ray.init();
  }

  @AfterMethod
  public void tearDown() {
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
    if (!executeCommand(stopCommand, 10)) {
      throw new RuntimeException("Couldn't stop ray cluster");
    }
  }

  @Test
  public void testMultiLanguageCluster() {
    RayObject<String> obj = Ray.call(MultiLanguageClusterTest::echo, "hello");
    Assert.assertEquals("hello", obj.get());
  }

}
