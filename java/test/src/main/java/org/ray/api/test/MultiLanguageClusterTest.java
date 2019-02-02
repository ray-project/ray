package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MultiLanguageClusterTest {

  public static final String PLASMA_STORE_SOCKET_NAME = "/tmp/ray/test/plasma_store_socket";
  public static final String RAYLET_SOCKET_NAME = "/tmp/ray/test/raylet_socket";

  @RayRemote
  public static String echo(String word) {
    return word;
  }

  @BeforeMethod
  public void setUp() throws InterruptedException, IOException {
    if (0 != startCluster()) {
      throw new SkipException("Since there is no ray command, we skip this test.");
    }

    System.setProperty("ray.home", "../..");
    System.setProperty("ray.redis.address", "127.0.0.1:6379");
    System.setProperty("ray.object-store.socket-name", PLASMA_STORE_SOCKET_NAME);
    System.setProperty("ray.raylet.socket-name", RAYLET_SOCKET_NAME);
    Ray.init();
  }

  @AfterMethod
  public void tearDown() throws IOException, InterruptedException {
    Ray.shutdown();
    System.clearProperty("ray.home");
    System.clearProperty("ray.redis.address");
    System.clearProperty("ray.object-store.socket-name");
    System.clearProperty("ray.raylet.socket-name");
    stopCluster();
  }

  @Test
  public void testEnableJavaAndPythonWorker() {
    RayObject<String> obj = Ray.call(MultiLanguageClusterTest::echo, "hello");
    Assert.assertEquals("hello", obj.get());
  }

  private int startCluster() throws IOException, InterruptedException {
    final List<String> startCommand = ImmutableList.of(
        "ray",
        "start",
        "--head",
        "--redis-port=6379",
        "--include-java",
        String.format("--plasma-store-socket-name=%s", PLASMA_STORE_SOCKET_NAME),
        String.format("--raylet-socket-name=%s", RAYLET_SOCKET_NAME),
        "--java-worker-options=-classpath ../../build/java/*:../../java/test/target/*"
    );

    ProcessBuilder builder = new ProcessBuilder(startCommand);
    Process process = builder.start();
    process.waitFor(10, TimeUnit.SECONDS);
    return process.exitValue();
  }

  private void stopCluster() throws IOException, InterruptedException {
    final List<String> stopCommand = ImmutableList.of(
        "ray",
        "stop"
    );

    ProcessBuilder builder = new ProcessBuilder(stopCommand);
    Process process = builder.start();
    process.waitFor(10, TimeUnit.SECONDS);

    // make sure `ray stop` command get finished. Otherwise some kill commands
    // will terminate the processes which is created by next test case.
    TimeUnit.MILLISECONDS.sleep(1500);
  }

}
