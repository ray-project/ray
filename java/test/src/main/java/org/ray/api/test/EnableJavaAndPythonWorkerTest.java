package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;
import org.ray.runtime.util.FileUtil;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EnableJavaAndPythonWorkerTest {

  public static final String PLASMA_STORE_SOCKET_NAME = "/tmp/ray/test/plasma_store_socket";
  public static final String RAYLET_SOCKET_NAME = "/tmp/ray/test/raylet_socket";

  @RayRemote
  public static String echo(String word) {
    return word;
  }

  @BeforeMethod
  public void setUp() {
    File baseDir = new File("/tmp/ray/test");
    FileUtil.mkDir(baseDir);
  }

  @AfterMethod
  public void tearDown() {
    //clean some files.
    File plasmaSocket = new File(PLASMA_STORE_SOCKET_NAME);
    if (plasmaSocket.exists()) {
      plasmaSocket.delete();
    }
  }

  @Test
  public void testEnableJavaAndPythonWorker() throws IOException, InterruptedException {
    // TODO(qwang): check `ray` command exist

    final String rayCommandPrefix = "/Users/wangqing/gits/X/ray/venv/bin";
    final List<String> startCommand = ImmutableList.of(
      String.format("%s/ray", rayCommandPrefix),
      "start",
      "--head",
      "--redis-port=6379",
      "--include-java",
      String.format("--plasma-store-socket-name=%s", PLASMA_STORE_SOCKET_NAME),
      String.format("--raylet-socket-name=%s", RAYLET_SOCKET_NAME),
      "--java-worker-options=-classpath /Users/wangqing/Workspace/ray/build/java/*:/Users/wangqing/Workspace/ray/java/test/target/*"
      );

    ProcessBuilder builder = new ProcessBuilder(startCommand);
    Process process = builder.start();
    process.waitFor(500, TimeUnit.MILLISECONDS);
    Assert.assertNotEquals(null, process);

    System.out.println(String.join(" ", startCommand));

    // test
    System.setProperty("ray.home", "../..");
    System.setProperty("ray.redis.address", "127.0.0.1:6379");
    System.setProperty("ray.object-store.socket-name", PLASMA_STORE_SOCKET_NAME);
    System.setProperty("ray.raylet.socket-name", RAYLET_SOCKET_NAME);
    Ray.init();


    RayObject<String> obj = Ray.call(EnableJavaAndPythonWorkerTest::echo, "hello");
    Assert.assertEquals("hello", obj.get());


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
