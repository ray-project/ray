package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.concurrent.TimeUnit;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.exception.RayException;
import org.ray.api.id.ObjectId;
import org.ray.runtime.RayObjectImpl;
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
    RayObject<String> notExisting = new RayObjectImpl(randomId);

    Thread thread = new Thread(() -> {
      try {
        TimeUnit.SECONDS.sleep(1);
        Ray.shutdown();
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
