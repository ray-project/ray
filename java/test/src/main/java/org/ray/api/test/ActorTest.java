package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.concurrent.TimeUnit;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.annotation.RayRemote;
import org.ray.api.exception.UnreconstructableException;
import org.ray.api.id.UniqueId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.RayActorImpl;
import org.ray.runtime.objectstore.ObjectStoreProxy.GetResult;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ActorTest extends BaseTest {

  @RayRemote
  public static class Counter {

    private int value;

    public Counter(int initValue) {
      this.value = initValue;
    }

    public int getValue() {
      return value;
    }

    public int increase(int delta) {
      value += delta;
      return value;
    }
  }

  @Test
  public void testCreateAndCallActor() {
    // Test creating an actor from a constructor
    RayActor<Counter> actor = Ray.createActor(Counter::new, 1);
    Assert.assertNotEquals(actor.getId(), UniqueId.NIL);
    // Test calling an actor
    Assert.assertEquals(Integer.valueOf(1), Ray.call(Counter::getValue, actor).get());
    Assert.assertEquals(Integer.valueOf(11), Ray.call(Counter::increase, actor, 10).get());
  }

  @RayRemote
  public static Counter factory(int initValue) {
    return new Counter(initValue);
  }

  @Test
  public void testCreateActorFromFactory() {
    // Test creating an actor from a factory method
    RayActor<Counter> actor = Ray.createActor(ActorTest::factory, 1);
    Assert.assertNotEquals(actor.getId(), UniqueId.NIL);
    // Test calling an actor
    Assert.assertEquals(Integer.valueOf(1), Ray.call(Counter::getValue, actor).get());
  }

  @RayRemote
  public static int testActorAsFirstParameter(RayActor<Counter> actor, int delta) {
    RayObject<Integer> res = Ray.call(Counter::increase, actor, delta);
    return res.get();
  }

  @RayRemote
  public static int testActorAsSecondParameter(int delta, RayActor<Counter> actor) {
    RayObject<Integer> res = Ray.call(Counter::increase, actor, delta);
    return res.get();
  }

  @Test
  public void testPassActorAsParameter() {
    RayActor<Counter> actor = Ray.createActor(Counter::new, 0);
    Assert.assertEquals(Integer.valueOf(1),
        Ray.call(ActorTest::testActorAsFirstParameter, actor, 1).get());
    Assert.assertEquals(Integer.valueOf(11),
        Ray.call(ActorTest::testActorAsSecondParameter, 10, actor).get());
  }

  @Test
  public void testForkingActorHandle() {
    RayActor<Counter> counter = Ray.createActor(Counter::new, 100);
    Assert.assertEquals(Integer.valueOf(101), Ray.call(Counter::increase, counter, 1).get());
    RayActor<Counter> counter2 = ((RayActorImpl<Counter>) counter).fork();
    Assert.assertEquals(Integer.valueOf(103), Ray.call(Counter::increase, counter2, 2).get());
  }

  @Test
  public void testUnreconstructableActorObject() throws InterruptedException {
    TestUtils.skipTestUnderSingleProcess();
    RayActor<Counter> counter = Ray.createActor(Counter::new, 100);
    // Call an actor method.
    RayObject value = Ray.call(Counter::getValue, counter);
    Assert.assertEquals(100, value.get());
    // Delete the object from the object store.
    Ray.internal().free(ImmutableList.of(value.getId()), false, false);
    // Wait until the object is deleted, because the above free operation is async.
    while (true) {
      GetResult<Integer> result = ((AbstractRayRuntime)
          Ray.internal()).getObjectStoreProxy().get(value.getId(), 0);
      if (!result.exists) {
        break;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }

    try {
      // Try getting the object again, this should throw an UnreconstructableException.
      value.get();
      Assert.fail("This line should not be reachable.");
    } catch (UnreconstructableException e) {
      Assert.assertEquals(value.getId(), e.objectId);
    }
  }
}
