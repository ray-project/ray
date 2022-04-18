package io.ray.test;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.parallelactor.*;
import io.ray.runtime.exception.RayActorException;
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

    public int add(int a, int b) {
      return a + b;
    }

    public int getThreadId() {
      return 1000000 + (int) Thread.currentThread().getId();
    }
  }

  /// 1. How to support concurrency group on parallel actor?
  /// 2. how to align all functionalities of ActorCaller/ActorTaskCaller, we need to extends from
  // ActorHandle?
  /// 3. How do we reuse APIs ActorCreator for parallelActorCreator, like setRuntimeEnv
  /// 4. The router is in the caller, to decide which index we should route. How to solve? Move to
  // callee?
  /// 5. We used function manager.
  public void testRoundRobinStrategy() {
    ParallelActor<A> actor = Parallel.actor(A::new).setParallels(10).remote();

    {
      // stateless tests
      ParallelInstance<A> instance = actor.getInstance(/*index=*/ 2);

      Preconditions.checkNotNull(instance);
      int res = instance.task(A::add, 100000, 200000).remote().get(); // Executed in instance 2
      Assert.assertEquals(res, 300000);

      instance = actor.getInstance(/*index=*/ 3);
      Preconditions.checkNotNull(instance);
      res = instance.task(A::add, 100000, 200000).remote().get(); // Executed in instance 2
      Assert.assertEquals(res, 300000);
    }

    {
      // stateful tests
      ParallelInstance<A> instance = actor.getInstance(/*index=*/ 2);

      Preconditions.checkNotNull(instance);
      int res = instance.task(A::incr, 1000000).remote().get(); // Executed in instance 2
      Assert.assertEquals(res, 1000000);

      instance = actor.getInstance(/*index=*/ 2);
      Preconditions.checkNotNull(instance);
      res = instance.task(A::incr, 2000000).remote().get(); // Executed in instance 2
      Assert.assertEquals(res, 3000000);
    }
  }

//  private static boolean passParallelActor(ParallelActor<A> parallelActor) {
//    ObjectRef<Integer> obj0 = parallelActor.task(A::incr, 1000000).remote();
//    ObjectRef<Integer> obj1 = parallelActor.task(A::incr, 2000000).remote();
//    // When parallel actor is passed in to a worker, the strategy should be erased
//    // because parallel actor strategy is work on caller side.
//    Assert.assertEquals(2000000, (int) obj0.get());
//    Assert.assertEquals(4000000, (int) obj1.get());
//    return true;
//  }
//
//  public void testPassParallelActorHandle() {
//    ParallelActor<A> actor =
//        Parallel.actor(A::new).setParallels(10).remote();
//    ObjectRef<Integer> obj0 = actor.task(A::incr, 1000000).remote();
//    ObjectRef<Integer> obj1 = actor.task(A::incr, 2000000).remote();
//    Assert.assertEquals(1000000, (int) obj0.get());
//    Assert.assertEquals(2000000, (int) obj1.get());
//    Assert.assertTrue(Ray.task(ParallelActorTest::passParallelActor, actor).remote().get());
//  }
//
//  public void testKillParallelActor() {
//    ParallelActor<A> actor =
//        Parallel.actor(A::new).setParallels(10).remote();
//    ObjectRef<Integer> obj0 = actor.task(A::incr, 1000000).remote();
//    Assert.assertEquals(1000000, (int) obj0.get());
//
//    ActorHandle<?> handle = actor.getHandle();
//    handle.kill(true);
//    final ObjectRef<Integer> obj1 = actor.task(A::incr, 1000000).remote();
//    Assert.expectThrows(RayActorException.class, obj1::get);
//  }
}
