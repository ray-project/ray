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

    private int value = 0;

    public int incr(int delta) {
      value += delta;
      return value;
    }
  }

  @RayRemote
  public static int testActorAsFirstParameter(RayActor<Counter> actor, int delta) {
    RayObject<Integer> res = Ray.call(Counter::incr, actor, delta);
    return res.get();
  }

  @RayRemote
  public static int testActorAsSecondParameter(int delta, RayActor<Counter> actor) {
    RayObject<Integer> res = Ray.call(Counter::incr, actor, delta);
    return res.get();
  }

  @Test
  public void testCreateAndCallActor() {
    // Test creating an actor
    RayActor<Counter> actor = Ray.createActor(Counter.class);
    Assert.assertNotEquals(actor.getId(), UniqueId.NIL);
    // Test calling an actor
    RayFunc2<Counter, Integer, Integer> f = Counter::incr;
    Assert.assertEquals(Integer.valueOf(1), Ray.call(f, actor, 1).get());
    Assert.assertEquals(Integer.valueOf(11), Ray.call(Counter::incr, actor, 10).get());
  }

  @Test
  public void testPassActorAsParameter() {
    RayActor<Counter> actor = Ray.createActor(Counter.class);
    RayFunc2<RayActor, Integer, Integer> f = ActorTest::testActorAsFirstParameter;
    Assert.assertEquals(Integer.valueOf(1),
        Ray.call(ActorTest::testActorAsFirstParameter, actor, 1).get());
    Assert.assertEquals(Integer.valueOf(11),
        Ray.call(ActorTest::testActorAsSecondParameter, 10, actor).get());
  }
}
