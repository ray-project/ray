package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.util.SystemUtil;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class MultiDriverTest extends BaseTest {

  private static final int DRIVER_COUNT = 10;
  private static final int NORMAL_TASK_COUNT_PER_DRIVER = 100;
  private static final int ACTOR_COUNT_PER_DRIVER = 10;
  private static final String PID_LIST_PREFIX = "PID: ";

  static int getPid() {
    return SystemUtil.pid();
  }

  public static class Actor {

    public int getPid() {
      return SystemUtil.pid();
    }
  }

  public static void main(String[] args) throws IOException {
    Ray.init();

    List<ObjectRef<Integer>> pidObjectList = new ArrayList<>();
    // Submit some normal tasks and get the PIDs of workers which execute the tasks.
    for (int i = 0; i < NORMAL_TASK_COUNT_PER_DRIVER; ++i) {
      pidObjectList.add(Ray.task(MultiDriverTest::getPid).remote());
    }
    // Create some actors and get the PIDs of actors.
    for (int i = 0; i < ACTOR_COUNT_PER_DRIVER; ++i) {
      ActorHandle<Actor> actor = Ray.actor(Actor::new).remote();
      pidObjectList.add(actor.task(Actor::getPid).remote());
    }
    Set<Integer> pids = new HashSet<>();
    for (ObjectRef<Integer> object : pidObjectList) {
      pids.add(object.get());
    }
    // Write pids to stdout
    System.out.println(
        PID_LIST_PREFIX + pids.stream().map(String::valueOf).collect(Collectors.joining(",")));
  }

  public void testMultiDrivers() throws InterruptedException, IOException {
    // This test case starts some driver processes. Each driver process submits some tasks and
    // collect the PIDs of the workers used by the driver. The drivers output the PID list
    // which will be read by the test case itself. The test case will compare the PIDs used by
    // different drivers and make sure that all the PIDs don't overlap. If overlapped, it means that
    // tasks owned by different drivers were scheduled to the same worker process, that is, tasks
    // of different jobs were not correctly isolated during execution.
    List<Process> drivers = new ArrayList<>();
    for (int i = 0; i < DRIVER_COUNT; ++i) {
      drivers.add(startDriver());
    }

    // Wait for drivers to finish.
    for (Process driver : drivers) {
      driver.waitFor();
      Assert.assertEquals(
          driver.exitValue(), 0, "The driver exited with code " + driver.exitValue());
    }

    // Read driver outputs and check for any PID overlap.
    Set<Integer> pids = new HashSet<>();
    for (Process driver : drivers) {
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(driver.getInputStream()))) {
        String line;
        int previousSize = pids.size();
        while ((line = reader.readLine()) != null) {
          if (line.startsWith(PID_LIST_PREFIX)) {
            for (String pidString : line.substring(PID_LIST_PREFIX.length()).split(",")) {
              // Make sure the PIDs don't overlap.
              Assert.assertTrue(
                  pids.add(Integer.valueOf(pidString)),
                  "Worker process with PID " + line + " is shared by multiple drivers.");
            }
            break;
          }
        }
        int nowSize = pids.size();
        Assert.assertTrue(nowSize > previousSize);
      }
    }
  }

  private Process startDriver() throws IOException {
    RayConfig rayConfig = TestUtils.getRuntime().getRayConfig();

    ProcessBuilder builder =
        new ProcessBuilder(
            "java",
            "-cp",
            System.getProperty("java.class.path"),
            "-Dray.address=" + rayConfig.getRedisAddress(),
            "-Dray.object-store.socket-name=" + rayConfig.objectStoreSocketName,
            "-Dray.raylet.socket-name=" + rayConfig.rayletSocketName,
            "-Dray.raylet.node-manager-port=" + String.valueOf(rayConfig.getNodeManagerPort()),
            MultiDriverTest.class.getName());
    builder.redirectError(Redirect.INHERIT);
    return builder.start();
  }
}
