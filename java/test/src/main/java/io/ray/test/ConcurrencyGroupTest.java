package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.concurrencygroup.ConcurrencyGroup;
import io.ray.api.concurrencygroup.ConcurrencyGroupBuilder;
import java.util.concurrent.CountDownLatch;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
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
}
