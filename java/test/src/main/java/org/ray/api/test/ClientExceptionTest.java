package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.annotation.RayRemote;
import org.ray.api.exception.RayException;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.ray.api.id.UniqueId;
import java.util.concurrent.TimeUnit;
import org.ray.runtime.RayObjectImpl;

public class ClientExceptionTest extends BaseTest {

  @Test
  public void testGet() {
    System.out.println("1");
    UniqueId randomId = UniqueId.randomId();
    RayObject<String> none_existent = new RayObjectImpl(randomId);
    System.out.println("2");

    Thread thread = new Thread(() -> {
      try {
        TimeUnit.SECONDS.sleep(1);
        Ray.shutdown();
      } catch (InterruptedException e) {
        System.out.println("Got InterruptedException when sleeping, exit right now.");
        throw new RuntimeException("Got InterruptedException when sleeping.", e);
      }
    });
    System.out.println("3");
    thread.start();
    System.out.println("4");
    try {
      none_existent.get();
      WaitResult<String> res = Ray.wait(ImmutableList.of(none_existent), 1, 2000);
      Assert.fail("Should not reach here");
    } catch (RayException e) {
      System.out.println(String.format("Expected runtime exception: {}", e));
    }
    System.out.println("5");
  }
}
