package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;
import org.ray.api.function.RayFunc2;
import org.ray.api.id.UniqueId;

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
    RayActor<Counter> counter2 = counter.fork(false);
    Assert.assertEquals(Integer.valueOf(103), Ray.call(Counter::increase, counter2, 2).get());
    RayActor<Counter> counter3 = counter2.fork(true);
    Assert.assertEquals(Integer.valueOf(106), Ray.call(Counter::increase, counter3, 3).get());
  }

}
