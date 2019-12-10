package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.api.TestUtils;
import org.ray.api.TestUtils.LargeObject;
import org.ray.api.annotation.RayRemote;
import org.ray.api.exception.UnreconstructableException;
import org.ray.api.id.UniqueId;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"directCall"})
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

    public void increase(int delta) {
      value += delta;
    }

    public int increaseAndGet(int delta) {
      value += delta;
      return value;
    }

    public int accessLargeObject(LargeObject largeObject) {
      value += largeObject.data.length;
      return value;
    }
  }

  public void testCreateAndCallActor() {
    // Test creating an actor from a constructor
    RayActor<Counter> actor = Ray.createActor(Counter::new, 1);
    Assert.assertNotEquals(actor.getId(), UniqueId.NIL);
    // A java actor is not a python actor
    Assert.assertFalse(actor instanceof RayPyActor);
    // Test calling an actor
    Assert.assertEquals(Integer.valueOf(1), Ray.call(Counter::getValue, actor).get());
    Ray.call(Counter::increase, actor, 1);
    Assert.assertEquals(Integer.valueOf(3), Ray.call(Counter::increaseAndGet, actor, 1).get());
  }

  /**
   * Test getting a direct object (an object that is returned by a direct-call task) twice from the
   * object store.
   *
   * Direct objects are stored in core worker's local memory. And it will be removed after the first
   * get. To enable getting it twice, we cache the object in `RayObjectImpl`.
   *
   * NOTE(hchen): this test will run for non-direct actors as well, which doesn't have the above
   * issue and should also succeed.
   */
  public void testGetDirectObjectTwice() {
    RayActor<Counter> actor = Ray.createActor(Counter::new, 1);
    RayObject<Integer> result = Ray.call(Counter::getValue, actor);
    Assert.assertEquals(result.get(), Integer.valueOf(1));
    Assert.assertEquals(result.get(), Integer.valueOf(1));
    // TODO(hchen): The following code will still fail, and can be fixed by using ref counting.
    // Assert.assertEquals(Ray.get(result.getId()), Integer.valueOf(1));
  }

  public void testCallActorWithLargeObject() {
    RayActor<Counter> actor = Ray.createActor(Counter::new, 1);
    LargeObject largeObject = new LargeObject();
    Assert.assertEquals(Integer.valueOf(largeObject.data.length + 1),
        Ray.call(Counter::accessLargeObject, actor, largeObject).get());
  }

  @RayRemote
  static Counter factory(int initValue) {
    return new Counter(initValue);
  }

  public void testCreateActorFromFactory() {
    // Test creating an actor from a factory method
    RayActor<Counter> actor = Ray.createActor(ActorTest::factory, 1);
    Assert.assertNotEquals(actor.getId(), UniqueId.NIL);
    // Test calling an actor
    Assert.assertEquals(Integer.valueOf(1), Ray.call(Counter::getValue, actor).get());
  }

  @RayRemote
  static int testActorAsFirstParameter(RayActor<Counter> actor, int delta) {
    RayObject<Integer> res = Ray.call(Counter::increaseAndGet, actor, delta);
    return res.get();
  }

  @RayRemote
  static int testActorAsSecondParameter(int delta, RayActor<Counter> actor) {
    RayObject<Integer> res = Ray.call(Counter::increaseAndGet, actor, delta);
    return res.get();
  }

  @RayRemote
  static int testActorAsFieldOfParameter(List<RayActor<Counter>> actor, int delta) {
    RayObject<Integer> res = Ray.call(Counter::increaseAndGet, actor.get(0), delta);
    return res.get();
  }

  public void testPassActorAsParameter() {
    RayActor<Counter> actor = Ray.createActor(Counter::new, 0);
    Assert.assertEquals(Integer.valueOf(1),
        Ray.call(ActorTest::testActorAsFirstParameter, actor, 1).get());
    Assert.assertEquals(Integer.valueOf(11),
        Ray.call(ActorTest::testActorAsSecondParameter, 10, actor).get());
    Assert.assertEquals(Integer.valueOf(111),
        Ray.call(ActorTest::testActorAsFieldOfParameter, Collections.singletonList(actor), 100)
            .get());
  }

  public void testUnreconstructableActorObject() throws InterruptedException {
    TestUtils.skipTestUnderSingleProcess();
    // The UnreconstructableException is created by raylet.
    // TODO (kfstorm): This should be supported by direct actor call.
    TestUtils.skipTestIfDirectActorCallEnabled();
    RayActor<Counter> counter = Ray.createActor(Counter::new, 100);
    // Call an actor method.
    RayObject value = Ray.call(Counter::getValue, counter);
    Assert.assertEquals(100, value.get());
    // Delete the object from the object store.
    Ray.internal().free(ImmutableList.of(value.getId()), false, false);
    // Wait until the object is deleted, because the above free operation is async.
    while (true) {
      Boolean result = TestUtils.getRuntime().getObjectStore()
          .wait(ImmutableList.of(value.getId()), 1, 0).get(0);
      if (!result) {
        break;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }

    try {
      // Try getting the object again, this should throw an UnreconstructableException.
      // Use `Ray.get()` to bypass the cache in `RayObjectImpl`.
      Ray.get(value.getId());
      Assert.fail("This line should not be reachable.");
    } catch (UnreconstructableException e) {
      Assert.assertEquals(value.getId(), e.objectId);
    }
  }
}
