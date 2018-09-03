package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;
import org.ray.api.function.RayFunc2;
import org.ray.api.id.UniqueId;

@RunWith(MyRunner.class)
public class ActorTest {

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
  public void testCreateAndCall() {
    // Test creating an actor
    RayActor<Counter> actor = Ray.createActor(Counter::new, 1);
    Assert.assertNotEquals(actor.getId(), UniqueId.NIL);
    // Test calling an actor
    Assert.assertEquals(Integer.valueOf(1), Ray.call(Counter::getValue, actor).get());
    Assert.assertEquals(Integer.valueOf(11), Ray.call(Counter::increase, actor, 10).get());
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
    RayFunc2<RayActor, Integer, Integer> f = ActorTest::testActorAsFirstParameter;
    Assert.assertEquals(Integer.valueOf(1),
        Ray.call(ActorTest::testActorAsFirstParameter, actor, 1).get());
    Assert.assertEquals(Integer.valueOf(11),
        Ray.call(ActorTest::testActorAsSecondParameter, 10, actor).get());
  }
}
