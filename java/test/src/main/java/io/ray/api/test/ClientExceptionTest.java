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
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClientExceptionTest extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientExceptionTest.class);

  @Test
  public void testWaitAndCrash() {
    TestUtils.skipTestUnderSingleProcess();
    ObjectId randomId = ObjectId.fromRandom();
    RayObject<String> notExisting = new RayObjectImpl(randomId, String.class);

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
      Ray.wait(ImmutableList.of(notExisting), 1, 2000);
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
