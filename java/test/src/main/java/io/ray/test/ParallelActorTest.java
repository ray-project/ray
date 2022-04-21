package io.ray.test;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.parallelactor.*;
import io.ray.runtime.exception.RayActorException;
import io.ray.runtime.util.SystemUtil;
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

    public int getPid() {
      // TODO(qwang): We should fix this once Serialization2.0 finished.
      return 1000000 + SystemUtil.pid();
    }

    public int getThreadId() {
      return 1000000 + (int) Thread.currentThread().getId();
    }
  }

  public void testBasic() {
    ParallelActorHandle<A> actor = ParallelActor.actor(A::new).setParallelism(10).remote();
    {
      // stateless tests
      ParallelActorInstance<A> instance = actor.getInstance(2);

      Preconditions.checkNotNull(instance);
      int res = instance.task(A::add, 100000, 200000).remote().get(); // Executed in instance 2
      Assert.assertEquals(res, 300000);

      instance = actor.getInstance(3);
      Preconditions.checkNotNull(instance);
      res = instance.task(A::add, 100000, 200000).remote().get(); // Executed in instance 2
      Assert.assertEquals(res, 300000);
    }

    {
      // stateful tests
      ParallelActorInstance<A> instance = actor.getInstance(2);

      Preconditions.checkNotNull(instance);
      int res = instance.task(A::incr, 1000000).remote().get(); // Executed in instance 2
      Assert.assertEquals(res, 1000000);

      instance = actor.getInstance(2);
      Preconditions.checkNotNull(instance);
      res = instance.task(A::incr, 2000000).remote().get(); // Executed in instance 2
      Assert.assertEquals(res, 3000000);
    }

    {
      // Test they are in the same process.
      ParallelActorInstance<A> instance = actor.getInstance(0);
      Preconditions.checkNotNull(instance);
      int pid1 = instance.task(A::getPid).remote().get();

      instance = actor.getInstance(1);
      Preconditions.checkNotNull(instance);
      int pid2 = instance.task(A::getPid).remote().get();
      Assert.assertEquals(pid1, pid2);
    }

    {
      // Test they are in different threads.
      ParallelActorInstance<A> instance = actor.getInstance(4);
      Preconditions.checkNotNull(instance);
      int thread1 = instance.task(A::getThreadId).remote().get();

      instance = actor.getInstance(5);
      Preconditions.checkNotNull(instance);
      int thread2 = instance.task(A::getThreadId).remote().get();
      Assert.assertNotEquals(thread1, thread2);
    }
  }

  private static boolean passParallelActor(ParallelActorHandle<A> parallelActorHandle) {
    ObjectRef<Integer> obj0 = parallelActorHandle.getInstance(0).task(A::incr, 1000000).remote();
    ObjectRef<Integer> obj1 = parallelActorHandle.getInstance(1).task(A::incr, 2000000).remote();
    Assert.assertEquals(2000000, (int) obj0.get());
    Assert.assertEquals(4000000, (int) obj1.get());
    return true;
  }

  public void testPassParallelActorHandle() {
    ParallelActorHandle<A> actor = ParallelActor.actor(A::new).setParallelism(10).remote();
    ObjectRef<Integer> obj0 = actor.getInstance(0).task(A::incr, 1000000).remote();
    ObjectRef<Integer> obj1 = actor.getInstance(1).task(A::incr, 2000000).remote();
    Assert.assertEquals(1000000, (int) obj0.get());
    Assert.assertEquals(2000000, (int) obj1.get());
    Assert.assertTrue(Ray.task(ParallelActorTest::passParallelActor, actor).remote().get());
  }

  public void testKillParallelActor() {
    ParallelActorHandle<A> actor = ParallelActor.actor(A::new).setParallelism(10).remote();
    ObjectRef<Integer> obj0 = actor.getInstance(0).task(A::incr, 1000000).remote();
    Assert.assertEquals(1000000, (int) obj0.get());

    ActorHandle<?> handle = actor.getHandle();
    handle.kill(true);
    final ObjectRef<Integer> obj1 = actor.getInstance(0).task(A::incr, 1000000).remote();
    Assert.expectThrows(RayActorException.class, obj1::get);
  }
}
