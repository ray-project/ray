package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.actor.NativeActorHandle;
import io.ray.runtime.exception.RayActorException;
import io.ray.runtime.util.SystemUtil;
import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class ActorHandleReferenceCountTest {

  private static final Logger LOG = LoggerFactory.getLogger(ActorHandleReferenceCountTest.class);

  /**
   * Because we can't explicitly GC an Java object. We use this helper method to manually remove an
   * local reference.
   */
  private static void del(ActorHandle<?> handle) {
    try {
      Field referencesField = NativeActorHandle.class.getDeclaredField("REFERENCES");
      referencesField.setAccessible(true);
      Set<?> references = (Set<?>) referencesField.get(null);
      Class<?> referenceClass =
          Class.forName("io.ray.runtime.actor.NativeActorHandle$NativeActorHandleReference");
      Method finalizeReferentMethod = referenceClass.getDeclaredMethod("finalizeReferent");
      finalizeReferentMethod.setAccessible(true);
      for (Object reference : references) {
        if (handle.equals(((Reference<?>) reference).get())) {
          finalizeReferentMethod.invoke(reference);
          break;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class MyActor {

    public int getPid() {
      return SystemUtil.pid();
    }

    public String hello() {
      return "hello";
    }
  }

  private static String foo(ActorHandle<MyActor> myActor, ActorHandle<SignalActor> signal) {
    signal.task(SignalActor::waitSignal).remote().get();
    String result = myActor.task(MyActor::hello).remote().get();
    del(myActor);
    return result;
  }

  public void testActorHandleReferenceCount() {
    try {
      System.setProperty("ray.job.num-java-workers-per-process", "1");
      Ray.init();
      ActorHandle<SignalActor> signal = Ray.actor(SignalActor::new).remote();
      ActorHandle<MyActor> myActor = Ray.actor(MyActor::new).remote();
      int pid = myActor.task(MyActor::getPid).remote().get();
      // Pass the handle to another task that cannot run yet.
      ObjectRef<String> helloObj =
          Ray.task(ActorHandleReferenceCountTest::foo, myActor, signal).remote();
      // Delete the original handle. The actor should not get killed yet.
      del(myActor);
      // Once the task finishes, the actor process should get killed.
      signal.task(SignalActor::sendSignal).remote().get();
      Assert.assertEquals("hello", helloObj.get());
      Assert.assertTrue(TestUtils.waitForCondition(() -> !SystemUtil.isProcessAlive(pid), 10000));
    } finally {
      Ray.shutdown();
    }
  }

  public void testRemoveActorHandleReferenceInMultipleThreadedActor() throws InterruptedException {
    System.setProperty("ray.job.num-java-workers-per-process", "5");
    try {
      Ray.init();
      ActorHandle<MyActor> myActor1 = Ray.actor(MyActor::new).remote();
      int pid1 = myActor1.task(MyActor::getPid).remote().get();
      ActorHandle<MyActor> myActor2 = Ray.actor(MyActor::new).remote();
      int pid2 = myActor2.task(MyActor::getPid).remote().get();
      Assert.assertEquals(pid1, pid2);
      del(myActor1);
      TimeUnit.SECONDS.sleep(5);
      Assert.assertThrows(
          RayActorException.class,
          () -> {
            myActor1.task(MyActor::hello).remote().get();
          });
      /// myActor2 shouldn't be killed.
      Assert.assertEquals("hello", myActor2.task(MyActor::hello).remote().get());
    } finally {
      Ray.shutdown();
    }
  }
}
