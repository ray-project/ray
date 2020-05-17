package io.ray.api.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.Ray;
import io.ray.api.RayObject;
import io.ray.api.TestUtils;
import io.ray.api.exception.RayException;
import io.ray.api.id.ObjectId;
import io.ray.runtime.RayNativeRuntime;
import io.ray.runtime.object.RayObjectImpl;
import io.ray.runtime.runner.RunManager;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClientExceptionTest extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientExceptionTest.class);

  private static String sleep() {
    try {
      Thread.sleep(3 * 1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
    char[] chars = new char[1024*1024];
    Arrays.fill(chars, 'x');
    return new String(chars);
  }

  @Test
  public void testWaitAndCrash() {
    TestUtils.skipTestUnderSingleProcess();
    RayObject<String> obj1 = Ray.call(ClientExceptionTest::sleep);

    Thread thread = new Thread(() -> {
      try {
        TimeUnit.SECONDS.sleep(1);
        // kill raylet
        RunManager runManager =
            ((RayNativeRuntime) TestUtils.getUnderlyingRuntime()).getRunManager();
        for (Process process : runManager.getProcesses("raylet")) {
          runManager.terminateProcess("raylet", process);
        }
      } catch (InterruptedException e) {
        LOGGER.error("Got InterruptedException when sleeping, exit right now.");
        throw new RuntimeException("Got InterruptedException when sleeping.", e);
      }
    });
    thread.start();
    try {
      Ray.wait(ImmutableList.of(obj1), 1, 10 * 1000);
      Assert.fail("Should not reach here");
    } catch (RayException e) {
      LOGGER.debug("Expected runtime exception: {}", e);
    }
    try {
      thread.join();
    } catch (Exception e) {
      LOGGER.error("Excpetion caught: {}", e);
    }
  }
}
