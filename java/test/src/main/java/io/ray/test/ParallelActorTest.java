package io.ray.test;

import io.ray.api.ObjectRef;
import io.ray.api.parallelactor.Parallel;
import io.ray.api.parallelactor.ParallelActor;
import io.ray.api.parallelactor.ParallelInstance;
import io.ray.api.parallelactor.ParallelStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
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
  /// 2. how to align all functionalities of ActorCaller/ActorTaskCaller, we need to extends from ActorHandle?
  /// 3. How do we reuse APIs ActorCreator for parallelActorCreator, like setRuntimeEnv
  /// 4. The router is in the caller, to decide which index we should route. How to solve? Move to callee?
  /// 5. We used function manager.
  public void testF() {
    ParallelActor<A> actor = Parallel.actor(A::new)
      .setParallels(ParallelStrategy.ROUND_ROBIN, 10).remote();

    /// TODO: Why we set the delta so large? since it's cast to short otherwise.
    ObjectRef<Integer> obj0 = actor.task(A::incr, 1000000).remote(); // 调用的是instance 0
    ObjectRef<Integer> obj1 = actor.task(A::incr, 1000000).remote(); // 调用的是instance 1
    ObjectRef<Integer> obj2 = actor.task(A::incr, 1000000).remote(); // 调用的是instance 2
    Assert.assertEquals(1000000, (int) obj0.get());
    Assert.assertEquals(1000000, (int) obj1.get());
    Assert.assertEquals(1000000, (int) obj2.get());

    ParallelInstance<A> instance = actor.getInstance(/*index=*/2);

    /// We haven't impled this, but it's the same implementation as above.
//    instance.task(A::f1, 100, 200).remote();   // Executed in instance 2
  }
}
