package io.ray.test;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.utils.parallelactor.Parallel;
import io.ray.api.utils.parallelactor.ParallelActor;
import io.ray.api.utils.parallelactor.ParallelInstance;
import io.ray.api.utils.parallelactor.ParallelStrategy;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class ParallelActorTest extends BaseTest {

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

  /// 问题1：该parallel actor支持concurrency group吗?
  /// 问题2：如何对齐和actor的所有功能，他需要是一个ActorHandle的子类吗?
  /// 问题3: parallelActorCreator需要集成于ActorCreator, 这样才能保证很多接口不重复写，例如setRuntimeEnv
  public void testF() {
    ParallelActor<A> actor = Parallel.parallelActor(A::new)
      .setParallels(ParallelStrategy.ROUND_ROBIN, 10).remote();

    ObjectRef<Integer> obj0 = actor.task(A::incr, 1).remote(); // 调用的是instance 0
    ObjectRef<Integer> obj1 = actor.task(A::incr, 1).remote(); // 调用的是instance 1
    ObjectRef<Integer> obj2 = actor.task(A::incr, 1).remote(); // 调用的是instance 2
    Assert.assertEquals(1, (int) obj0.get());
    Assert.assertEquals(1, (int) obj1.get());
    Assert.assertEquals(1, (int) obj2.get());

    ParallelInstance<A> instance = actor.getParallel(/*index=*/2);
    instance.task(A::f1, 100, 200).remote();   // Executed in instance 2
  }

}
