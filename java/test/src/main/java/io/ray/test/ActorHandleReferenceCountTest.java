package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.actor.NativeActorHandle;
import io.ray.runtime.util.SystemUtil;
import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Set;
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
}
