package io.ray.api.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.TestUtils;
import io.ray.api.options.ActorCreationOptions;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class ActorConcurrentCallTest extends BaseTest {

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
    TestUtils.skipTestUnderSingleProcess();

    ActorCreationOptions op = new ActorCreationOptions.Builder()
        .setMaxConcurrency(3)
        .createActorCreationOptions();
    ActorHandle<ConcurrentActor> actor = Ray.createActor(ConcurrentActor::new, op);
    ObjectRef<String> obj1 = actor.call(ConcurrentActor::countDown);
    ObjectRef<String> obj2 = actor.call(ConcurrentActor::countDown);
    ObjectRef<String> obj3 = actor.call(ConcurrentActor::countDown);

    List<Integer> expectedResult = ImmutableList.of(1, 2, 3);
    Assert.assertEquals(obj1.get(), "ok");
    Assert.assertEquals(obj2.get(), "ok");
    Assert.assertEquals(obj3.get(), "ok");
  }

}
