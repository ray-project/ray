package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.concurrencygroup.ConcurrencyGroup;
import io.ray.api.concurrencygroup.ConcurrencyGroupBuilder;
import io.ray.api.concurrencygroup.annotations.DefConcurrencyGroup;
import io.ray.api.concurrencygroup.annotations.UseConcurrencyGroup;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class ConcurrencyGroupTest extends BaseTest {

  private static class ConcurrentActor {
    public long f1() {
      return Thread.currentThread().getId();
    }

    public long f2() {
      return Thread.currentThread().getId();
    }

    public long f3(int a, int b) {
      return Thread.currentThread().getId();
    }

    public long f4() {
      return Thread.currentThread().getId();
    }

    public long f5() {
      return Thread.currentThread().getId();
    }

    public long f6() {
      return Thread.currentThread().getId();
    }
  }

  public void testLimitMethodsInOneGroup() {
    ConcurrencyGroup group1 =
        new ConcurrencyGroupBuilder<ConcurrentActor>()
            .setName("io")
            .setMaxConcurrency(1)
            .addMethod(ConcurrentActor::f1)
            .addMethod(ConcurrentActor::f2)
            .build();

    ConcurrencyGroup group2 =
        new ConcurrencyGroupBuilder<ConcurrentActor>()
            .setName("executing")
            .setMaxConcurrency(1)
            .addMethod(ConcurrentActor::f3)
            .addMethod(ConcurrentActor::f4)
            .build();

    ActorHandle<ConcurrentActor> myActor =
        Ray.actor(ConcurrentActor::new).setConcurrencyGroups(group1, group2).remote();

    long threadId1 = myActor.task(ConcurrentActor::f1).remote().get();
    long threadId2 = myActor.task(ConcurrentActor::f2).remote().get();
    long threadId3 = myActor.task(ConcurrentActor::f3, 3, 5).remote().get();
    long threadId4 = myActor.task(ConcurrentActor::f4).remote().get();
    long threadId5 = myActor.task(ConcurrentActor::f5).remote().get();
    long threadId6 = myActor.task(ConcurrentActor::f6).remote().get();
    long threadId7 =
        myActor.task(ConcurrentActor::f1).setConcurrencyGroup("executing").remote().get();

    Assert.assertEquals(threadId1, threadId2);
    Assert.assertEquals(threadId3, threadId4);
    Assert.assertEquals(threadId5, threadId6);
    Assert.assertNotEquals(threadId1, threadId3);
    Assert.assertNotEquals(threadId1, threadId5);
    Assert.assertNotEquals(threadId3, threadId5);
    Assert.assertEquals(threadId3, threadId7);
  }

  private static class CountDownActor {
    // The countdown for f1, f2.
    private final CountDownLatch countDownLatch1 = new CountDownLatch(2);

    // The countdown for f3, f4.
    private final CountDownLatch countDownLatch2 = new CountDownLatch(4);

    // The countdown for default method f5.
    private final CountDownLatch countDownLatch3 = new CountDownLatch(2);

    private static boolean countDown(CountDownLatch countDownLatch) {
      countDownLatch.countDown();
      try {
        countDownLatch.await();
        return true;
      } catch (InterruptedException e) {
        return false;
      }
    }

    public boolean f1(double a) {
      return countDown(countDownLatch1);
    }

    public boolean f2(int a, double b) {
      return countDown(countDownLatch1);
    }

    public boolean f3(int a, int b, double c) {
      return countDown(countDownLatch2);
    }

    public boolean f4(double a, int b, double c, int d) {
      return countDown(countDownLatch2);
    }

    public boolean f5() {
      return countDown(countDownLatch3);
    }
  }

  public void testMaxConcurrencyForGroups() {
    ConcurrencyGroup group1 =
        new ConcurrencyGroupBuilder<CountDownActor>()
            .setName("io")
            .setMaxConcurrency(4)
            .addMethod(CountDownActor::f1)
            .addMethod(CountDownActor::f2)
            .build();

    ConcurrencyGroup group2 =
        new ConcurrencyGroupBuilder<CountDownActor>()
            .setName("executing")
            .setMaxConcurrency(4)
            .addMethod(CountDownActor::f3)
            .addMethod(CountDownActor::f4)
            .build();

    ActorHandle<CountDownActor> myActor =
        Ray.actor(CountDownActor::new)
            .setConcurrencyGroups(group1, group2)
            .setMaxConcurrency(3)
            .remote();

    ObjectRef<Boolean> ret1 = myActor.task(CountDownActor::f1, 2.0).remote();
    ObjectRef<Boolean> ret2 = myActor.task(CountDownActor::f2, 1, 2.0).remote();
    Assert.assertTrue(ret1.get());
    Assert.assertTrue(ret2.get());

    ObjectRef<Boolean> ret3 = myActor.task(CountDownActor::f3, 3, 5, 2.0).remote();
    ObjectRef<Boolean> ret4 = myActor.task(CountDownActor::f4, 1.0, 2, 3.0, 4).remote();
    ObjectRef<Boolean> ret5 = myActor.task(CountDownActor::f3, 3, 5, 2.0).remote();
    ObjectRef<Boolean> ret6 = myActor.task(CountDownActor::f4, 1.0, 2, 3.0, 4).remote();
    Assert.assertTrue(ret3.get());
    Assert.assertTrue(ret4.get());
    Assert.assertTrue(ret5.get());
    Assert.assertTrue(ret6.get());

    ObjectRef<Boolean> ret7 = myActor.task(CountDownActor::f5).remote();
    ObjectRef<Boolean> ret8 = myActor.task(CountDownActor::f5).remote();
    Assert.assertTrue(ret7.get());
    Assert.assertTrue(ret8.get());
  }

  private static class ConcurrencyActor2 {

    public String f1() throws InterruptedException {
      TimeUnit.MINUTES.sleep(100);
      return "never returned";
    }

    public String f2() {
      return "ok";
    }
  }

  /// This case tests that blocking task in default group will block other groups.
  /// See https://github.com/ray-project/ray/issues/20475
  @Test(groups = {"cluster"})
  public void testDefaultCgDoNotBlockOthers() {
    ConcurrencyGroup group =
        new ConcurrencyGroupBuilder<ConcurrencyActor2>()
            .setName("group")
            .setMaxConcurrency(1)
            .addMethod(ConcurrencyActor2::f2)
            .build();

    ActorHandle<ConcurrencyActor2> myActor =
        Ray.actor(ConcurrencyActor2::new).setConcurrencyGroups(group).remote();
    myActor.task(ConcurrencyActor2::f1).remote();
    Assert.assertEquals(myActor.task(ConcurrencyActor2::f2).remote().get(), "ok");
  }

  /// This case tests that the blocking concurrency group doesn't block the scheduling of
  /// other concurrency groups. See https://github.com/ray-project/ray/issues/19593 for details.
  @Test(groups = {"cluster"})
  public void testBlockingCgNotBlockOthers() {
    ConcurrencyGroup group1 =
        new ConcurrencyGroupBuilder<ConcurrencyActor2>()
            .setName("group1")
            .setMaxConcurrency(1)
            .addMethod(ConcurrencyActor2::f1)
            .build();

    ConcurrencyGroup group2 =
        new ConcurrencyGroupBuilder<ConcurrencyActor2>()
            .setName("group2")
            .setMaxConcurrency(1)
            .addMethod(ConcurrencyActor2::f2)
            .build();

    ActorHandle<ConcurrencyActor2> myActor =
        Ray.actor(ConcurrencyActor2::new).setConcurrencyGroups(group1, group2).remote();

    // Execute f1 twice. and the cg1 is blocking, but cg2 should work well.
    ObjectRef<String> obj0 = myActor.task(ConcurrencyActor2::f1).remote();
    ObjectRef<String> obj1 = myActor.task(ConcurrencyActor2::f1).remote();
    // Wait a while to make sure f2 is scheduled after f1.
    Ray.wait(ImmutableList.of(obj0, obj1), 2, 5 * 1000);

    // f2 should work well even if group1 is blocking.
    Assert.assertEquals(myActor.task(ConcurrencyActor2::f2).remote().get(), "ok");
  }

  @DefConcurrencyGroup(name = "io", maxConcurrency = 1)
  @DefConcurrencyGroup(name = "compute", maxConcurrency = 1)
  private static class StaticDefinedConcurrentActor {
    @UseConcurrencyGroup(name = "io")
    public long f1() {
      return Thread.currentThread().getId();
    }

    @UseConcurrencyGroup(name = "io")
    public long f2() {
      return Thread.currentThread().getId();
    }

    @UseConcurrencyGroup(name = "compute")
    public long f3(int a, int b) {
      return Thread.currentThread().getId();
    }

    @UseConcurrencyGroup(name = "compute")
    public long f4() {
      return Thread.currentThread().getId();
    }

    public long f5() {
      return Thread.currentThread().getId();
    }

    public long f6() {
      return Thread.currentThread().getId();
    }
  }

  private static class ChildActor extends StaticDefinedConcurrentActor {}

  public void testLimitMethodsInOneGroupOfStaticDefinition() {
    ActorHandle<ChildActor> myActor = Ray.actor(ChildActor::new).remote();
    long threadId1 = myActor.task(StaticDefinedConcurrentActor::f1).remote().get();
    long threadId2 = myActor.task(StaticDefinedConcurrentActor::f2).remote().get();
    long threadId3 = myActor.task(StaticDefinedConcurrentActor::f3, 3, 5).remote().get();
    long threadId4 = myActor.task(StaticDefinedConcurrentActor::f4).remote().get();
    long threadId5 = myActor.task(StaticDefinedConcurrentActor::f5).remote().get();
    long threadId6 = myActor.task(StaticDefinedConcurrentActor::f6).remote().get();
    Assert.assertEquals(threadId1, threadId2);
    Assert.assertEquals(threadId3, threadId4);
    Assert.assertEquals(threadId5, threadId6);
    Assert.assertNotEquals(threadId1, threadId3);
    Assert.assertNotEquals(threadId1, threadId5);
    Assert.assertNotEquals(threadId3, threadId5);
  }
}
