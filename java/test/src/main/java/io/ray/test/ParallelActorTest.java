package io.ray.test;

import com.google.common.base.Preconditions;
import io.ray.api.ObjectRef;
import io.ray.api.parallelactor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "cluster")
public class ParallelActorTest extends BaseTest {

  private static final Logger LOG = LoggerFactory.getLogger(ParallelActorTest.class);

  private static class A {
    private int value = 0;

    public int incr(int delta) {
      value += delta;
      return value;
    }

    public int getValue() {
      return value;
    }

    public int f1(int a, int b) {
      return a + b;
    }
  }

  /// 1. How to support concurrency group on parallel actor?
  /// 2. how to align all functionalities of ActorCaller/ActorTaskCaller, we need to extends from
  // ActorHandle?
  /// 3. How do we reuse APIs ActorCreator for parallelActorCreator, like setRuntimeEnv
  /// 4. The router is in the caller, to decide which index we should route. How to solve? Move to
  // callee?
  /// 5. We used function manager.
  public void testF() {
    ParallelActor<A> actor =
        Parallel.actor(A::new).setParallels(ParallelStrategy.ROUND_ROBIN, 10).remote();

    /// TODO: Why we set the delta so large? since it's cast to short otherwise.
    ObjectRef<Integer> obj0 = actor.task(A::incr, 1000000).remote(); // 调用的是instance 0
    ObjectRef<Integer> obj1 = actor.task(A::incr, 1000000).remote(); // 调用的是instance 1
    ObjectRef<Integer> obj2 = actor.task(A::incr, 1000000).remote(); // 调用的是instance 2
    Assert.assertEquals(1000000, (int) obj0.get());
    Assert.assertEquals(1000000, (int) obj1.get());
    Assert.assertEquals(1000000, (int) obj2.get());

    {
      // stateless tests
      ParallelInstance<A> instance = actor.getInstance(/*index=*/ 2);

      Preconditions.checkNotNull(instance);
      int res = instance.task(A::f1, 100000, 200000).remote().get(); // Executed in instance 2
      Assert.assertEquals(res, 300000);

      instance = actor.getInstance(/*index=*/ 3);
      Preconditions.checkNotNull(instance);
      res = instance.task(A::f1, 100000, 200000).remote().get(); // Executed in instance 2
      Assert.assertEquals(res, 300000);
    }

    {
      // stateful tests
      ParallelInstance<A> instance = actor.getInstance(/*index=*/ 2);

      Preconditions.checkNotNull(instance);
      int res = instance.task(A::incr, 1000000).remote().get(); // Executed in instance 2
      Assert.assertEquals(res, 2000000);

      instance = actor.getInstance(/*index=*/ 2);
      Preconditions.checkNotNull(instance);
      res = instance.task(A::incr, 2000000).remote().get(); // Executed in instance 2
      Assert.assertEquals(res, 4000000);
    }
  }
}
