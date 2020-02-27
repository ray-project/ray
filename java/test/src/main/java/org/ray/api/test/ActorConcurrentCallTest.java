package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.annotation.RayRemote;
import org.ray.api.options.ActorCreationOptions;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class ActorConcurrentCallTest extends BaseTest {

  @RayRemote
  public static class ConcurrentActor {
    private final CountDownLatch countDownLatch = new CountDownLatch(3);

    public String countDown() {
      countDownLatch.countDown();
      try {
        countDownLatch.await();
        return "ok";
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void testConcurrentCall() {
    TestUtils.skipTestIfDirectActorCallDisabled();

    ActorCreationOptions op = new ActorCreationOptions.Builder()
        .setMaxConcurrency(3)
        .createActorCreationOptions();
    RayActor<ConcurrentActor> actor = Ray.createActor(ConcurrentActor::new, op);
    RayObject<String> obj1 = Ray.call(ConcurrentActor::countDown, actor);
    RayObject<String> obj2 = Ray.call(ConcurrentActor::countDown, actor);
    RayObject<String> obj3 = Ray.call(ConcurrentActor::countDown, actor);

    List<Integer> expectedResult = ImmutableList.of(1, 2, 3);
    Assert.assertEquals(obj1.get(), "ok");
    Assert.assertEquals(obj2.get(), "ok");
    Assert.assertEquals(obj3.get(), "ok");
  }

}
