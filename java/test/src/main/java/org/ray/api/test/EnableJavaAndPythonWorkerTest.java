package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import org.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EnableJavaAndPythonWorkerTest {

  @Test
  public void testEnableJavaAndPythonWorker() throws IOException, InterruptedException {
    // TODO(qwang): check `ray` command exist

    final String rayCommandPrefix = "/Users/wangqing/gits/X/ray/venv/bin";
    final String plasmaStoreSocket = "/tmp/ray/test/plasma_store_socket";
    final String rayletSocket = "/tmp/ray/test/raylet_socket";
    final List<String> startCommand = ImmutableList.of(
      String.format("%s/ray", rayCommandPrefix),
      "start",
      "--head",
      "--redis-port=6379",
      "--include-java",
      String.format("--plasma-store-socket-name=%s", plasmaStoreSocket),
      String.format("--raylet-socket-name=%s", rayletSocket)
      //"--java-worker-options=-cp=../../build/java/*"
      );

    ProcessBuilder builder = new ProcessBuilder(startCommand);
    Process process = builder.start();
    process.waitFor(500, TimeUnit.MILLISECONDS);
    Assert.assertNotEquals(null, process);

    System.out.println(String.join(" ", startCommand));

    // test
    System.setProperty("ray.home", "../..");
    System.setProperty("ray.redis.address", "127.0.0.1:6379");
    System.setProperty("ray.object-store.socket-name", plasmaStoreSocket);
    System.setProperty("ray.raylet.socket-name", rayletSocket);
    Ray.init();



    Ray.shutdown();
    System.clearProperty("ray.home");
    System.clearProperty("ray.redis.address");


    // stop
    final List<String> stopCommand = ImmutableList.of(
      String.format("%s/ray", rayCommandPrefix),
      "stop"
    );

    builder = new ProcessBuilder(stopCommand);
    process = builder.start();
    process.waitFor(500, TimeUnit.MILLISECONDS);
  }

  private void startCluster() {

  }

  private void stopCluster() {

  }
}
