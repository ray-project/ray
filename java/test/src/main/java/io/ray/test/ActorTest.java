package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.exception.UnreconstructableException;
import io.ray.api.id.ActorId;
import io.ray.api.id.UniqueId;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class ActorTest extends BaseTest {

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

    public int accessLargeObject(TestUtils.LargeObject largeObject) {
      value += largeObject.data.length;
      return value;
    }
  }

  public void testCreateAndCallActor() {
    // Test creating an actor from a constructor
    ActorHandle<Counter> actor = Ray.actor(Counter::new, 1).remote();
    Assert.assertNotEquals(actor.getId(), ActorId.NIL);
    // A java actor is not a python actor
    Assert.assertFalse(actor instanceof PyActorHandle);
    // Test calling an actor
    Assert.assertEquals(Integer.valueOf(1), actor.task(Counter::getValue).remote().get());
    actor.task(Counter::increase, 1).remote();
    Assert.assertEquals(Integer.valueOf(3),
        actor.task(Counter::increaseAndGet, 1).remote().get());
  }

  /**
   * Test getting an object twice from the local memory store.
   *
   * Objects are stored in core worker's local memory. And it will be removed after the first
   * get. To enable getting it twice, we cache the object in `RayObjectImpl`.
   */
  public void testGetObjectTwice() {
    ActorHandle<Counter> actor = Ray.actor(Counter::new, 1).remote();
    ObjectRef<Integer> result = actor.task(Counter::getValue).remote();
    Assert.assertEquals(result.get(), Integer.valueOf(1));
    Assert.assertEquals(result.get(), Integer.valueOf(1));
    // TODO(hchen): The following code will still fail, and can be fixed by using ref counting.
    // Assert.assertEquals(Ray.get(result.getId()), Integer.valueOf(1));
  }

  public void testCallActorWithLargeObject() {
    ActorHandle<Counter> actor = Ray.actor(Counter::new, 1).remote();
    TestUtils.LargeObject largeObject = new TestUtils.LargeObject();
    Assert.assertEquals(Integer.valueOf(largeObject.data.length + 1),
        actor.task(Counter::accessLargeObject, largeObject).remote().get());
  }

  static Counter factory(int initValue) {
    return new Counter(initValue);
  }

  public void testCreateActorFromFactory() {
    // Test creating an actor from a factory method
    ActorHandle<Counter> actor = Ray.actor(ActorTest::factory, 1).remote();
    Assert.assertNotEquals(actor.getId(), UniqueId.NIL);
    // Test calling an actor
    Assert.assertEquals(Integer.valueOf(1), actor.task(Counter::getValue).remote().get());
  }

  static int testActorAsFirstParameter(ActorHandle<Counter> actor, int delta) {
    ObjectRef<Integer> res = actor.task(Counter::increaseAndGet, delta).remote();
    return res.get();
  }

  static int testActorAsSecondParameter(int delta, ActorHandle<Counter> actor) {
    ObjectRef<Integer> res = actor.task(Counter::increaseAndGet, delta).remote();
    return res.get();
  }

  static int testActorAsFieldOfParameter(List<ActorHandle<Counter>> actor, int delta) {
    ObjectRef<Integer> res = actor.get(0).task(Counter::increaseAndGet, delta).remote();
    return res.get();
  }

  public void testPassActorAsParameter() {
    ActorHandle<Counter> actor = Ray.actor(Counter::new, 0).remote();
    Assert.assertEquals(Integer.valueOf(1),
        Ray.task(ActorTest::testActorAsFirstParameter, actor, 1).remote().get());
    Assert.assertEquals(Integer.valueOf(11),
        Ray.task(ActorTest::testActorAsSecondParameter, 10, actor).remote().get());
    Assert.assertEquals(Integer.valueOf(111),
        Ray.task(ActorTest::testActorAsFieldOfParameter,
            Collections.singletonList(actor), 100).remote().get());
  }

  // TODO(qwang): Will re-enable this test case once ref counting is supported in Java.
  @Test(enabled = false, groups = {"cluster"})
  public void testUnreconstructableActorObject() throws InterruptedException {
    // The UnreconstructableException is created by raylet.
    ActorHandle<Counter> counter = Ray.actor(Counter::new, 100).remote();
    // Call an actor method.
    ObjectRef value = counter.task(Counter::getValue).remote();
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
      Ray.get(value.getId(), value.getType());
      Assert.fail("This line should not be reachable.");
    } catch (UnreconstructableException e) {
      Assert.assertEquals(value.getId(), e.objectId);
    }
  }
}
